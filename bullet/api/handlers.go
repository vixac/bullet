package api

import (
	"net/http"

	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store"

	"github.com/gin-gonic/gin"
)

var kvStore store.Store

func SetupRouter(store store.Store) *gin.Engine {
	kvStore = store
	r := gin.Default()
	r.POST("/insert-one", putHandler)
	r.POST("/insert-many", putManyHandler)
	r.POST("/get-many", getManyHandler)
	r.POST("/get-one", getHandler)
	r.POST("/delete-one", deleteHandler)
	return r
}

func putHandler(c *gin.Context) {
	var req model.KVRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := kvStore.Put(req.AppID, req.BucketID, req.Key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}
func putManyHandler(c *gin.Context) {
	var req model.PutManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	items := make(map[int32][]model.KeyValueItem)
	for _, bucket := range req.Buckets {
		items[bucket.BucketID] = append(items[bucket.BucketID], bucket.Items...)
	}

	if err := kvStore.PutMany(req.AppID, items); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}
func getManyHandler(c *gin.Context) {
	var req model.GetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	keys := make(map[int32][]string)
	for _, bucket := range req.Buckets {
		keys[bucket.BucketID] = append(keys[bucket.BucketID], bucket.Keys...)
	}

	values, missing, err := kvStore.GetMany(req.AppID, keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"values":  values,
		"missing": missing,
	})
}

func getHandler(c *gin.Context) {
	print("VX: get called")
	var req model.KVRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	value, err := kvStore.Get(req.AppID, req.BucketID, req.Key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": value})
}

func deleteHandler(c *gin.Context) {
	var req model.KVRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := kvStore.Delete(req.AppID, req.BucketID, req.Key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}
