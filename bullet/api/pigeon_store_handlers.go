package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store"
)

var pigeonStore store.PigeonStore

func SetupPigeonRouter(store store.PigeonStore) *gin.Engine {
	pigeonStore = store
	r := gin.Default()
	prefix := "/pigeon-store"
	r.POST(prefix+"/insert-one", pigeonPutHandler)
	r.POST(prefix+"/insert-many", pigeonPutManyHandler)
	r.POST(prefix+"/get-many", pigeonGetManyHandler)
	r.POST(prefix+"/get-one", pigeonGetHandler)
	r.POST(prefix+"/delete-one", pigeonDeleteHandler)
	return r
}

func pigeonPutHandler(c *gin.Context) {
	var req model.PigeonRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := pigeonStore.PigeonPut(req.AppID, req.Key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func pigeonPutManyHandler(c *gin.Context) {
	var req model.PigeonPutManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := pigeonStore.PigeonPutMany(req.AppID, req.Items); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}

func pigeonGetManyHandler(c *gin.Context) {
	var req model.PigeonGetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	values, missing, err := pigeonStore.PigeonGetMany(req.AppID, req.Keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"values":  values,
		"missing": missing,
	})
}

func pigeonGetHandler(c *gin.Context) {
	var req model.PigeonRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	value, err := pigeonStore.PigeonGet(req.AppID, req.Key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": value})
}

func pigeonDeleteHandler(c *gin.Context) {
	var req model.PigeonRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := pigeonStore.PigeonDelete(req.AppID, req.Key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}
