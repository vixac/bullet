package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store"
)

var depotStore store.DepotStore

func SetupDepotRouter(store store.DepotStore, prefix string, engine *gin.Engine) *gin.Engine {
	depotStore = store
	engine.POST(prefix+"/insert-one", depotPutHandler)
	engine.POST(prefix+"/insert-many", depotPutManyHandler)
	engine.POST(prefix+"/get-many", depotGetManyHandler)
	engine.POST(prefix+"/get-one", depotGetHandler)
	engine.POST(prefix+"/delete-one", depotDeleteHandler)
	return engine
}

func depotPutHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.DepotRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := depotStore.DepotPut(appId, req.Key, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func depotPutManyHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.DepotPutManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := depotStore.DepotPutMany(appId, req.Items); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusOK)
}

func depotGetManyHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.DepotGetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	values, missing, err := depotStore.DepotGetMany(appId, req.Keys)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"values":  values,
		"missing": missing,
	})
}

func depotGetHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.DepotRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	value, err := depotStore.DepotGet(appId, req.Key)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"value": value})
}

func depotDeleteHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}
	var req model.DepotRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := depotStore.DepotDelete(appId, req.Key); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}
