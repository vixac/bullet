package model

type KVRequest struct {
	AppID    int32  `json:"appId"`
	BucketID int32  `json:"bucketId"`
	Key      string `json:"key"`
	Value    int64  `json:"value,omitempty"`
}
