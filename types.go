package syncengine

// SyncItem 是唯一要求的實作資料介面
type SyncItem interface {
	Key() string      // 唯一識別：如 orderNumber
	Group() string    // 分群 key：如 JPID、partition ID
	Timestamp() int64 // 可用於檢查 timeout，UnixNano
}

type AsyncExecutor interface {
	Submit(task func())
}
