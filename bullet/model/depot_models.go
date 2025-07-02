package model

type DepotRequest struct {
	Key   int64  `json:"key"`
	Value string `json:"value,omitempty"`
}

type DepotKeyValueItem struct {
	Key   int64  `json:"key"`
	Value string `json:"value"`
}

type DepotPutManyRequest struct {
	Items []DepotKeyValueItem `json:"items"`
}

type DepotGetManyRequest struct {
	Keys []int64 `json:"keys"`
}
