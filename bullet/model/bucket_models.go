package model

type BucketRequest struct {
	BucketID int32  `json:"bucketId"`
	Key      string `json:"key"`
	Value    int64  `json:"value,omitempty"`
}

type PutManyRequest struct {
	Buckets []BucketPutItems `json:"buckets"`
}

type GetManyRequest struct {
	Buckets []BucketGetKeys `json:"buckets"`
}

type GetItemsByPrefixRequest struct {
	BucketID int32  `json:"bucketId"`
	Prefix   string `json:"prefix"`
}

type BucketKeyValueItem struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}

type BucketPutItems struct {
	BucketID int32                `json:"bucketId"`
	Items    []BucketKeyValueItem `json:"items"`
}

type BucketGetKeys struct {
	BucketID int32    `json:"bucketId"`
	Keys     []string `json:"keys"`
}

type GetManyResponse struct {
	Values  map[string]map[string]int64 `json:"values"`  // bucketId -> (key -> value)
	Missing map[string][]string         `json:"missing"` // bucketId -> list of missing keys
}
