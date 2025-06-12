package api

import (
	"net/http"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store"

	"github.com/gin-gonic/gin"
)

var bucketStore store.BucketStore

func SetupRouter(store store.BucketStore) *gin.Engine {
	bucketStore = store
	r := gin.Default()
	bucket := "/bucket-store"
	r.POST(bucket+"/insert-one", bucketPutHandler)
	r.POST(bucket+"/insert-many", bucketPutManyHandler)
	r.POST(bucket+"/get-many", bucketGetManyHandler)
	r.POST(bucket+"/get-one", bucketGetHandler)
	r.POST(bucket+"/delete-one", bucketDeleteHandler)
	return r
}

func bucketPutHandler(c *gin.Context) {
	var req model.KVRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := bucketStore.BucketPut(req.AppID, req.BucketID, req.Key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}
func bucketPutManyHandler(c *gin.Context) {
	var req model.PutManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	items := make(map[int32][]model.BucketKeyValueItem)
	for _, bucket := range req.Buckets {
		items[bucket.BucketID] = append(items[bucket.BucketID], bucket.Items...)
	}

	if err := bucketStore.BucketPutMany(req.AppID, items); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}
func bucketGetManyHandler(c *gin.Context) {
	var req model.GetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	keys := make(map[int32][]string)
	for _, bucket := range req.Buckets {
		keys[bucket.BucketID] = append(keys[bucket.BucketID], bucket.Keys...)
	}

	values, missing, err := bucketStore.BucketGetMany(req.AppID, keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"values":  values,
		"missing": missing,
	})
}

func bucketGetHandler(c *gin.Context) {
	print("VX: get called")
	var req model.KVRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	value, err := bucketStore.BucketGet(req.AppID, req.BucketID, req.Key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": value})
}

func bucketDeleteHandler(c *gin.Context) {
	var req model.KVRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := bucketStore.BucketDelete(req.AppID, req.BucketID, req.Key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}
