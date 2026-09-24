package protocol

type TrackMutateRequest struct {
	MutationID string               `json:"mutationId"`
	Puts       []TrackRequest       `json:"puts"`
	Deletes    []TrackBucketKeyPair `json:"deletes"`
}

type TrackMutateResponse struct {
	Applied bool `json:"applied"`
}

type TrackRequest struct {
	BucketID int32    `json:"bucketId"`
	Key      string   `json:"key"`
	Value    int64    `json:"value,string"`
	Tag      *int64   `json:"tag,omitempty"`
	Metric   *float64 `json:"metric,omitempty"`
	Payload  *[]byte  `json:"payload,omitempty"`
	// IfAbsent is supported by TrackMutate only; it makes this put create-only.
	IfAbsent bool `json:"ifAbsent,omitempty"`
}
type TrackDeleteManyRequest struct {
	Items []TrackBucketKeyPair `json:"items"`
}

type TrackPutManyRequest struct {
	Buckets []TrackPutItems `json:"buckets"`
}

type TrackGetManyRequest struct {
	Buckets        []TrackGetKeys `json:"buckets"`
	IncludePayload bool           `json:"includePayload,omitempty"`
}

type TrackGetRequest struct {
	BucketID       int32  `json:"bucketId"`
	Key            string `json:"key"`
	IncludePayload bool   `json:"includePayload,omitempty"`
}

type MetricFilter struct {
	Operator string  `json:"operator"` // "gt", "lt", etc.
	Value    float64 `json:"value"`
}
type TrackGetItemsByPrefixRequest struct {
	BucketID int32         `json:"bucketId"`
	Prefix   string        `json:"prefix"`
	Tags     []int64       `json:"tags,omitempty"`
	Metric   *MetricFilter `json:"metric,omitempty"`
}

type TrackGetItemsByPrefixesRequest struct {
	BucketID int32         `json:"bucketId"`
	Prefixes []string      `json:"prefixes"`
	Tags     []int64       `json:"tags,omitempty"`
	Metric   *MetricFilter `json:"metric,omitempty"`
}

type TrackBucketKeyPair struct {
	BucketID int32  `json:"bucketId"`
	Key      string `json:"key"`
}
type TrackKeyValueItem struct {
	Key   string     `json:"key"`
	Value TrackValue `json:"value"`
}

type TrackPutItems struct {
	BucketID int32               `json:"bucketId"`
	Items    []TrackKeyValueItem `json:"items"`
}

type TrackGetKeys struct {
	BucketID int32    `json:"bucketId"`
	Keys     []string `json:"keys"`
}

type TrackGetManyResponse struct {
	Values  map[string]map[string]TrackValue `json:"values"`  // bucketId -> (key -> value)
	Missing map[string][]string              `json:"missing"` // bucketId -> list of missing keys
}

type TrackValue struct {
	Value  int64    `json:"Value"`
	Tag    *int64   `json:"Tag"`
	Metric *float64 `json:"Metric"`
	// A nil pointer omits an unrequested payload. A pointer to a nil slice emits
	// null (requested but absent), while an empty slice emits an empty base64 string.
	Payload *[]byte `json:"Payload,omitempty"`
}
