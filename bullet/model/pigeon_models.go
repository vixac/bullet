package model

type PigeonRequest struct {
	Key   int64  `json:"key"`
	Value string `json:"value,omitempty"`
}

type PigeonKeyValueItem struct {
	Key   int64  `json:"key"`
	Value string `json:"value"`
}

type PigeonPutManyRequest struct {
	Items []PigeonKeyValueItem `json:"items"`
}

type PigeonGetManyRequest struct {
	Keys []int64 `json:"keys"`
}
