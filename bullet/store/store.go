package store

type Store interface {
    Put(appID, bucketID int32, key string, value int64) error
    Get(appID, bucketID int32, key string) (int64, error)
    Delete(appID, bucketID int32, key string) error
    Close() error
}
