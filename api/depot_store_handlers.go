package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

var depotStore store_interface.DepotStore

func SetupDepotRouter(store store_interface.DepotStore, prefix string, engine *gin.Engine) *gin.Engine {
	depotStore = store
	engine.POST(prefix+"/create-one", depotCreateHandler)
	engine.POST(prefix+"/create-many", depotCreateManyHandler)
	engine.POST(prefix+"/update", depotUpdateHandler)
	engine.POST(prefix+"/get-one", depotGetHandler)
	engine.POST(prefix+"/get-many", depotGetManyHandler)
	engine.POST(prefix+"/delete-one", depotDeleteHandler)
	engine.POST(prefix+"/delete-by-bucket", depotDeleteByBucketHandler)
	engine.POST(prefix+"/get-all-by-bucket", depotGetAllByBucketHandler)
	return engine
}

func depotCreateHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.DepotCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	space := store_interface.TenancySpace{AppId: appId, TenancyId: 0}
	id, err := depotStore.DepotCreate(space, req.BucketID, req.Value)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, model.DepotCreateResponse{ID: id})
}

func depotCreateManyHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.DepotCreateManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	space := store_interface.TenancySpace{AppId: appId, TenancyId: 0}
	ids, err := depotStore.DepotCreateMany(space, req.BucketID, req.Values)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, model.DepotCreateManyResponse{IDs: ids})
}

func depotUpdateHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.DepotUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	space := store_interface.TenancySpace{AppId: appId, TenancyId: 0}
	if err := depotStore.DepotUpdate(space, req.ID, req.Value); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func depotGetHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.DepotGetRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	space := store_interface.TenancySpace{AppId: appId, TenancyId: 0}
	value, err := depotStore.DepotGet(space, req.ID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, model.DepotGetResponse{Value: value})
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

	space := store_interface.TenancySpace{AppId: appId, TenancyId: 0}
	values, missing, err := depotStore.DepotGetMany(space, req.IDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, model.DepotGetManyResponse{Values: values, Missing: missing})
}

func depotDeleteHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.DepotDeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	space := store_interface.TenancySpace{AppId: appId, TenancyId: 0}
	if err := depotStore.DepotDelete(space, req.ID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func depotDeleteByBucketHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.DepotBucketRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	space := store_interface.TenancySpace{AppId: appId, TenancyId: 0}
	if err := depotStore.DepotDeleteByBucket(space, req.BucketID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusOK)
}

func depotGetAllByBucketHandler(c *gin.Context) {
	appId, err := extractAppIDFromHeader(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "missing or invalid app ID"})
		return
	}

	var req model.DepotBucketRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	space := store_interface.TenancySpace{AppId: appId, TenancyId: 0}
	values, err := depotStore.DepotGetAllByBucket(space, req.BucketID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, model.DepotGetAllByBucketResponse{Values: values})
}
