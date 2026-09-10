package protocol

import "github.com/vixac/bullet/model"

// Model returns the transport-independent value.
func (v TrackValue) Model() model.TrackValue {
	return model.TrackValue{Value: v.Value, Tag: v.Tag, Metric: v.Metric}
}

func TrackValueFromModel(v model.TrackValue) TrackValue {
	return TrackValue{Value: v.Value, Tag: v.Tag, Metric: v.Metric}
}

func TrackItemsFromModel(items []model.TrackKeyValueItem) []TrackKeyValueItem {
	if items == nil {
		return nil
	}
	result := make([]TrackKeyValueItem, len(items))
	for i, item := range items {
		result[i] = TrackKeyValueItem{Key: item.Key, Value: TrackValueFromModel(item.Value)}
	}
	return result
}

// Model combines repeated bucket entries just as the HTTP API does.
func (r TrackPutManyRequest) Model() map[int32][]model.TrackKeyValueItem {
	result := make(map[int32][]model.TrackKeyValueItem)
	for _, bucket := range r.Buckets {
		if _, ok := result[bucket.BucketID]; !ok {
			result[bucket.BucketID] = nil
		}
		for _, item := range bucket.Items {
			result[bucket.BucketID] = append(result[bucket.BucketID], model.TrackKeyValueItem{Key: item.Key, Value: item.Value.Model()})
		}
	}
	return result
}

func (r TrackDeleteManyRequest) Model() []model.TrackKey {
	result := make([]model.TrackKey, len(r.Items))
	for i, item := range r.Items {
		result[i] = model.TrackKey{BucketID: item.BucketID, Key: item.Key}
	}
	return result
}

// TrackGetResponse is the body returned by POST /track/items/get.
type TrackGetResponse struct {
	Value int64 `json:"value"`
}

// TrackQueryResponse is shared by the single- and multi-prefix query endpoints.
type TrackQueryResponse struct {
	Items []TrackKeyValueItem `json:"items"`
}
