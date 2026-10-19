package store

// Store is everything the cluster layer needs from a storage engine.
// Bitcask is one implementation; tests can plug in a fake one.
//
// Every write carries a timestamp, and the coordinator (not the store)
// chooses it. Two rules follow from that:
//   - Put/Delete with a timestamp OLDER than what is stored are ignored
//     (last-write-wins). They return nil, not an error.
//   - Get on a deleted key returns ErrNotFound together with the delete's
//     timestamp, so the coordinator can still compare versions. For a key
//     that was never written the timestamp is 0.
type Store interface {
	Get(key string) (value []byte, ts int64, err error)
	Put(key string, value []byte, ts int64) error
	Delete(key string, ts int64) error
}

// Compile-time check: if Bitcask ever stops matching Store, the build
// fails right here instead of somewhere far away.
var _ Store = (*Bitcask)(nil)