package model

/*
*
VX:TODO list
  - rename bucket requset and pigeon
  - the entire project needs renaming.

- if we keep it bullet. then we need to remove bucket. too similar.
- so app is bullet. maybe i should call it something db?
- quickdb
*/
type BucketRequest struct {
	BucketID int32    `json:"bucketId"`
	Key      string   `json:"key"`
	Value    int64    `json:"value"`
	Tag      *int64   `json:"tag,omitempty"`
	Metric   *float64 `json:"metric,omitempty"`
}

type PutManyRequest struct {
	Buckets []BucketPutItems `json:"buckets"`
}

type GetManyRequest struct {
	Buckets []BucketGetKeys `json:"buckets"`
}

type MetricFilter struct {
	Operator string  `json:"operator"` // "gt", "lt", etc.
	Value    float64 `json:"value"`
}
type GetItemsByPrefixRequest struct {
	BucketID int32         `json:"bucketId"`
	Prefix   string        `json:"prefix"`
	Tags     []int64       `json:"tags,omitempty"`   // optional IN clause
	Metric   *MetricFilter `json:"metric,omitempty"` // optional metric filter
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

type BucketValue struct {
	Value  int64    `bson:"value"`
	Tag    *int64   `bson:"tag,omitempty"`
	Metric *float64 `bson:"metric,omitempty"`
}
