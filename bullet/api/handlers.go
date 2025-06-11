package api

import (
	"bullet/model"
	"bullet/store"
	"net/http"

	"github.com/gin-gonic/gin"
)

var kvStore store.Store

func SetupRouter(store store.Store) *gin.Engine {
	kvStore = store
	r := gin.Default()
	r.POST("/put", putHandler)
	r.POST("/get", getHandler)
	r.POST("/delete", deleteHandler)
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

func getHandler(c *gin.Context) {
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
