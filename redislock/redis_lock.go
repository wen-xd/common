package distlock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrLockFailed 加锁失败
	ErrLockFailed = errors.New("failed to acquire lock")
	// ErrLockNotHeld 锁未持有
	ErrLockNotHeld = errors.New("lock not held")
	// ErrLockAlreadyHeld 锁已被持有
	ErrLockAlreadyHeld = errors.New("lock already held")
)

// RedisLock Redis 分布式锁
type RedisLock struct {
	client redis.Cmdable
	name   string        // 锁名称
	token  string        // 锁令牌(唯一标识)
	ttl    time.Duration // 锁过期时间

	// 自动续期相关
	watchdogInterval time.Duration      // 续期间隔
	watchdogCancel   context.CancelFunc // 取消续期
	watchdogDone     chan struct{}      // 续期完成信号
}

// LockOption 锁配置选项
type LockOption func(*RedisLock)

// WithTTL 设置锁的过期时间(默认 30 秒)
func WithTTL(ttl time.Duration) LockOption {
	return func(l *RedisLock) {
		l.ttl = ttl
	}
}

// WithWatchdog 启用自动续期(默认不启用)
// interval: 续期间隔,建议设置为 TTL 的 1/3
func WithWatchdog(interval time.Duration) LockOption {
	return func(l *RedisLock) {
		l.watchdogInterval = interval
	}
}

// NewRedisLock 创建 Redis 分布式锁
// name: 锁名称,用于定位作用范围
// opts: 可选配置
func NewRedisLock(client redis.Cmdable, name string, opts ...LockOption) *RedisLock {
	lock := &RedisLock{
		client:       client,
		name:         fmt.Sprintf("lock:%s", name),
		token:        generateToken(),
		ttl:          30 * time.Second, // 默认 30 秒
		watchdogDone: make(chan struct{}),
	}

	// 应用配置选项
	for _, opt := range opts {
		opt(lock)
	}

	return lock
}

// Lock 加锁(阻塞直到成功或超时)
// ctx: 上下文,用于控制超时和取消
func (l *RedisLock) Lock(ctx context.Context) error {
	return l.LockWithRetry(ctx, 0, 100*time.Millisecond)
}

// TryLock 尝试加锁(非阻塞)
// 成功返回 nil,失败返回 ErrLockFailed
func (l *RedisLock) TryLock(ctx context.Context) error {
	acquired, err := l.acquire(ctx)
	if err != nil {
		return fmt.Errorf("try lock failed: %w", err)
	}
	if !acquired {
		return ErrLockFailed
	}

	// 启动自动续期
	if l.watchdogInterval > 0 {
		l.startWatchdog()
	}

	return nil
}

// LockWithRetry 加锁并重试
// maxRetries: 最大重试次数(0 表示无限重试,直到 ctx 取消)
func (l *RedisLock) LockWithRetry(ctx context.Context, maxRetries int, retryInterval time.Duration) error {
	retries := 0
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		acquired, err := l.acquire(ctx)
		if err != nil {
			return fmt.Errorf("lock failed: %w", err)
		}

		if acquired {
			// 启动自动续期
			if l.watchdogInterval > 0 {
				l.startWatchdog()
			}
			return nil
		}

		// 检查是否达到最大重试次数
		if maxRetries > 0 {
			retries++
			if retries >= maxRetries {
				return ErrLockFailed
			}
		}

		// 等待重试或上下文取消
		select {
		case <-ctx.Done():
			return fmt.Errorf("lock cancelled: %w", ctx.Err())
		case <-ticker.C:
			// 继续重试
		}
	}
}

// Unlock 解锁
func (l *RedisLock) Unlock(ctx context.Context) error {
	// 停止自动续期
	l.stopWatchdog()

	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`

	result, err := l.client.Eval(ctx, script, []string{l.name}, l.token).Result()
	if err != nil {
		return fmt.Errorf("unlock failed: %w", err)
	}

	if result.(int64) == 0 {
		return ErrLockNotHeld
	}

	return nil
}

// Refresh 手动续期
func (l *RedisLock) Refresh(ctx context.Context) error {
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	result, err := l.client.Eval(ctx, script, []string{l.name}, l.token, l.ttl.Milliseconds()).Result()
	if err != nil {
		return fmt.Errorf("refresh failed: %w", err)
	}

	if result.(int64) == 0 {
		return ErrLockNotHeld
	}

	return nil
}

// acquire 尝试获取锁
func (l *RedisLock) acquire(ctx context.Context) (bool, error) {
	result, err := l.client.SetNX(ctx, l.name, l.token, l.ttl).Result()
	if err != nil {
		return false, err
	}
	return result, nil
}

// startWatchdog 启动自动续期
func (l *RedisLock) startWatchdog() {
	ctx, cancel := context.WithCancel(context.Background())
	l.watchdogCancel = cancel

	go func() {
		defer close(l.watchdogDone)

		ticker := time.NewTicker(l.watchdogInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C: // 续期
				refreshCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				err := l.Refresh(refreshCtx)
				cancel()

				if err != nil {
					if errors.Is(err, ErrLockNotHeld) {
						return
					}
				}
			}
		}
	}()
}

// stopWatchdog 停止自动续期
func (l *RedisLock) stopWatchdog() {
	if l.watchdogCancel != nil {
		l.watchdogCancel()
		<-l.watchdogDone // 等待 watchdog 完全停止
	}
}

// generateToken 生成唯一令牌
func generateToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// WithLock 使用分布式锁执行函数(便捷方法)
// 自动处理加锁、解锁和错误恢复
func WithLock(ctx context.Context, lock *RedisLock, fn func() error) error {
	// 加锁
	if err := lock.Lock(ctx); err != nil {
		return fmt.Errorf("acquire lock failed: %w", err)
	}

	// 确保解锁
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := lock.Unlock(unlockCtx); err != nil {
		}
	}()

	// 执行业务逻辑
	return fn()
}
