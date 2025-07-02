package model

type WayFinderQueryItem struct {
	Key     string   `json:"key"`
	ItemId  int64    `json:"itemId"`
	Tag     *int64   `json:"tag,omitempty"`
	Metric  *float64 `json:"value,omitempty"`
	Payload string   `json:"payload"`
}
