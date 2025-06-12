package model

type PigeonRequest struct {
	AppID int32  `json:"appId"`
	Key   int64  `json:"key"`
	Value string `json:"value,omitempty"`
}

type PigeonKeyValueItem struct {
	Key   int64  `json:"key"`
	Value string `json:"value"`
}

type PigeonPutManyRequest struct {
	AppID int32                `json:"appId"`
	Items []PigeonKeyValueItem `json:"items"`
}

type PigeonGetManyRequest struct {
	AppID int32   `json:"appId"`
	Keys  []int64 `json:"keys"`
}
