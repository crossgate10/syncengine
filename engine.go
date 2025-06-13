package syncengine

import (
	"sync"
	"time"
)

type SyncEngine struct {
	cache      sync.Map // map[group]string]*itemGroup
	cfg        Config
	dispatchCh chan []SyncItem
	stop       chan struct{}
}

type itemGroup struct {
	mu       sync.Mutex
	items    map[string]SyncItem // key -> item
	attempts map[string]int      // key -> retry count
	lastSent int64               // UnixNano
}

type LoggerFunc func(format string, args ...any)

type Config struct {
	BatchSize  int                                  // 單次觸發發送的最小項目數，例如 5：超過就立刻送出
	MaxRetry   int                                  // 單一資料最大重試次數，超過後就不再派發
	FlushAfter time.Duration                        // 每筆超過這個等待時間，即使未滿也會 flush
	RetryAfter time.Duration                        // 上次發送超過多久後才允許 retry
	Tick       time.Duration                        // 引擎內部輪詢週期，影響 dispatch/retry 頻率
	OnDispatch func(group string, batch []SyncItem) // 外部注入的發送函式：當資料可送時會呼叫此函式

	Executor AsyncExecutor // Optional: 預設為 DefaultExecutor，可傳入自定義 goroutine pool
	Logger   LoggerFunc    // Optional
}

func NewEngine(cfg Config) *SyncEngine {
	return &SyncEngine{
		cfg:        cfg,
		dispatchCh: make(chan []SyncItem),
		stop:       make(chan struct{}),
	}
}

func (e *SyncEngine) Start() {
	go e.dispatchLoop()
	go e.retryLoop()
}

func (e *SyncEngine) Stop() {
	close(e.stop)
}

func (e *SyncEngine) Add(item SyncItem) {
	raw, _ := e.cache.LoadOrStore(item.Group(), &itemGroup{
		items:    make(map[string]SyncItem),
		attempts: make(map[string]int),
	})
	ig := raw.(*itemGroup)

	ig.mu.Lock()
	defer ig.mu.Unlock()
	ig.items[item.Key()] = item
}

func (e *SyncEngine) Ack(group string, keys []string) {
	val, ok := e.cache.Load(group)
	if !ok {
		return
	}
	ig := val.(*itemGroup)

	ig.mu.Lock()
	defer ig.mu.Unlock()
	for _, k := range keys {
		delete(ig.items, k)
		delete(ig.attempts, k)
	}
}

func (e *SyncEngine) dispatchLoop() {
	ticker := time.NewTicker(e.cfg.Tick)
	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano()
			e.cache.Range(func(k, v any) bool {
				ig := v.(*itemGroup)

				ig.mu.Lock()
				unsent := make([]SyncItem, 0)
				for key, it := range ig.items {
					if ig.attempts[key] == 0 {
						unsent = append(unsent, it)
					}
				}
				shouldFlush := len(unsent) >= e.cfg.BatchSize || now-ig.lastSent > e.cfg.FlushAfter.Nanoseconds()
				if len(unsent) > 0 && shouldFlush {
					// 只要有資料，且超過間隔或數量，皆可送
					batch := unsent[:min(e.cfg.BatchSize, len(unsent))]

					for _, b := range batch {
						ig.attempts[b.Key()]++
					}
					ig.lastSent = time.Now().UnixNano()

					executor := e.cfg.Executor
					if executor == nil {
						executor = &DefaultExecutor{}
					}

					executor.Submit(func() {
						defer func() {
							if r := recover(); r != nil {
								e.cfg.Logger("[WARN] dispatch panic: %v", r)
							}
						}()
						e.cfg.Logger("start dispatch...")
						e.cfg.OnDispatch(k.(string), batch)
					})
				}
				ig.mu.Unlock()
				return true
			})
		case <-e.stop:
			return
		}
	}
}

func (e *SyncEngine) retryLoop() {
	ticker := time.NewTicker(e.cfg.Tick)
	for {
		select {
		case <-ticker.C:
			now := time.Now().UnixNano()
			e.cache.Range(func(k, v any) bool {
				group := k.(string)
				ig := v.(*itemGroup)

				ig.mu.Lock()
				if len(ig.items) == 0 {
					ig.mu.Unlock()
					return true
				}
				if ig.lastSent == 0 {
					ig.mu.Unlock()
					return true
				}
				if now-ig.lastSent < e.cfg.RetryAfter.Nanoseconds() {
					ig.mu.Unlock()
					return true
				}

				retryable := make([]SyncItem, 0)
				for key, it := range ig.items {
					if ig.attempts[key] < e.cfg.MaxRetry {
						ig.attempts[key]++
						retryable = append(retryable, it)
					}
				}
				if len(retryable) > 0 {
					ig.lastSent = now

					executor := e.cfg.Executor
					if executor == nil {
						executor = &DefaultExecutor{}
					}

					executor.Submit(func() {
						defer func() {
							if r := recover(); r != nil {
								e.cfg.Logger("[WARN] dispatch panic: %v", r)
							}
						}()
						e.cfg.Logger("start retrying...")
						e.cfg.OnDispatch(group, retryable)
					})
				}
				ig.mu.Unlock()
				return true
			})
		case <-e.stop:
			return
		}
	}
}
