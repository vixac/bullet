package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	store_interface "github.com/vixac/bullet/store/store_interface"
)

type depotHandler struct {
	store store_interface.DepotStore
}

// SetupDepotRouter registers all depot endpoints under the given prefix.
//
// Endpoints:
//
//	POST   {prefix}/items            — create one
//	POST   {prefix}/items/batch      — create many
//	PUT    {prefix}/items/:id        — update
//	GET    {prefix}/items/:id        — get one
//	POST   {prefix}/items/batch-get  — get many (IDs in body)
//	DELETE {prefix}/items/:id        — delete one
//	DELETE {prefix}/bucket/:bucketId — delete by bucket
//	GET    {prefix}/bucket/:bucketId — get all by bucket
func SetupDepotRouter(store store_interface.DepotStore, prefix string, engine *gin.Engine) *gin.Engine {
	h := &depotHandler{store: store}
	g := engine.Group(prefix)
	g.POST("/items", h.createOne)
	g.POST("/items/batch", h.createMany)
	g.PUT("/items/:id", h.update)
	g.GET("/items/:id", h.getOne)
	g.POST("/items/batch-get", h.getMany)
	g.DELETE("/items/:id", h.deleteOne)
	g.DELETE("/bucket/:bucketId", h.deleteByBucket)
	g.GET("/bucket/:bucketId", h.getAllByBucket)
	return engine
}

func (h *depotHandler) createOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.DepotCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	id, err := h.store.DepotCreate(space, req.BucketID, req.Value)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, model.DepotCreateResponse{ID: id})
}

func (h *depotHandler) createMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.DepotCreateManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ids, err := h.store.DepotCreateMany(space, req.BucketID, req.Values)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, model.DepotCreateManyResponse{IDs: ids})
}

func (h *depotHandler) update(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var req model.DepotUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := h.store.DepotUpdate(space, id, req.Value); err != nil {
		respondError(c, err)
		return
	}
	c.Status(http.StatusOK)
}

func (h *depotHandler) getOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	value, err := h.store.DepotGet(space, id)
	if err != nil {
		respondError(c, err)
		return
	}
	c.JSON(http.StatusOK, model.DepotGetResponse{Value: value})
}

func (h *depotHandler) getMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.DepotGetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	values, missing, err := h.store.DepotGetMany(space, req.IDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, model.DepotGetManyResponse{Values: values, Missing: missing})
}

func (h *depotHandler) deleteOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	if err := h.store.DepotDelete(space, id); err != nil {
		respondError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *depotHandler) deleteByBucket(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	bucketID, err := strconv.ParseInt(c.Param("bucketId"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid bucketId"})
		return
	}
	if err := h.store.DepotDeleteByBucket(space, int32(bucketID)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *depotHandler) getAllByBucket(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	bucketID, err := strconv.ParseInt(c.Param("bucketId"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid bucketId"})
		return
	}
	values, err := h.store.DepotGetAllByBucket(space, int32(bucketID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, model.DepotGetAllByBucketResponse{Values: values})
}
