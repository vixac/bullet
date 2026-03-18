package model

type DepotCreateRequest struct {
	BucketID int32  `json:"bucket_id"`
	Value    string `json:"value"`
}

type DepotCreateResponse struct {
	ID int64 `json:"id"`
}

type DepotCreateManyRequest struct {
	BucketID int32    `json:"bucket_id"`
	Values   []string `json:"values"`
}

type DepotCreateManyResponse struct {
	IDs []int64 `json:"ids"`
}

type DepotUpdateRequest struct {
	ID    int64  `json:"id"`
	Value string `json:"value"`
}

type DepotGetRequest struct {
	ID int64 `json:"id"`
}

type DepotGetResponse struct {
	Value string `json:"value"`
}

type DepotGetManyRequest struct {
	IDs []int64 `json:"ids"`
}

type DepotGetManyResponse struct {
	Values  map[int64]string `json:"values"`
	Missing []int64          `json:"missing"`
}

type DepotDeleteRequest struct {
	ID int64 `json:"id"`
}

type DepotBucketRequest struct {
	BucketID int32 `json:"bucket_id"`
}

type DepotGetAllByBucketResponse struct {
	Values map[int64]string `json:"values"`
}
