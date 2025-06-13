package syncengine

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

type MyItem struct {
	id      string
	group   string
	sentAt  time.Time
	content string
}

func (m *MyItem) Key() string      { return m.id }
func (m *MyItem) Group() string    { return m.group }
func (m *MyItem) Timestamp() int64 { return m.sentAt.UnixNano() }

func ExampleSyncEngine() {
	var engine *SyncEngine
	engine = NewEngine(Config{
		BatchSize:  20,
		MaxRetry:   3,
		FlushAfter: 1 * time.Second,
		RetryAfter: 10 * time.Second,
		Tick:       2 * time.Second,
		OnDispatch: func(group string, batch []SyncItem) {
			for _, it := range batch {
				if mi, ok := it.(*MyItem); ok {
					log.Printf("  -> send %s, content: %s\n", it.Key(), mi.content)
				}
			}
			// 模擬 ACK
			var ackIds []string
			for i, it := range batch {
				if it.Key() == "ORD_000" {
					// 模擬 Retry 情境
					continue
				}
				if i%2 == 0 {
					ackIds = append(ackIds, it.Key())
				}
			}
			engine.Ack(group, ackIds)
			log.Printf("  -> ack %v\n", ackIds)
		},
		Logger: func(format string, args ...any) {
			log.Printf("[SyncEngine] "+format, args...)
		},
	})
	engine.Start()
	defer engine.Stop()

	// 模擬加入
	for i := 0; i <= 10; i++ {
		engine.Add(&MyItem{
			id:      fmt.Sprintf("ORD_%03d", i),
			group:   "JP101",
			sentAt:  time.Now(),
			content: strconv.Itoa(i),
		})
	}
	log.Println("add done")

	time.AfterFunc(35*time.Second, func() {
		engine.Ack("JP101", []string{"ORD_000"})
		log.Println("  -> ack ORD_000")
	})

	select {}
	// output:
}
