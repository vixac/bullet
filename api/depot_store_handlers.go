package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/protocol"
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
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.DepotCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	id, err := h.store.DepotCreate(space, req.BucketID, req.Value)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "depot", "written", 1)
	c.JSON(http.StatusCreated, protocol.DepotCreateResponse{ID: id})
}

func (h *depotHandler) createMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.DepotCreateManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	ids, err := h.store.DepotCreateMany(space, req.BucketID, req.Values)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "depot", "written", len(ids))
	c.JSON(http.StatusCreated, protocol.DepotCreateManyResponse{IDs: ids})
}

func (h *depotHandler) update(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{Error: "invalid id"})
		return
	}
	var req protocol.DepotUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	if err := h.store.DepotUpdate(space, id, req.Value); err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "depot", "written", 1)
	c.Status(http.StatusOK)
}

func (h *depotHandler) getOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{Error: "invalid id"})
		return
	}
	value, err := h.store.DepotGet(space, id)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "depot", "read", 1)
	c.JSON(http.StatusOK, protocol.DepotGetResponse{Value: value})
}

func (h *depotHandler) getMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	var req protocol.DepotGetManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
		return
	}
	values, missing, err := h.store.DepotGetMany(space, req.IDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "depot", "read", len(values))
	c.JSON(http.StatusOK, protocol.DepotGetManyResponse{Values: values, Missing: missing})
}

func (h *depotHandler) deleteOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{Error: "invalid id"})
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
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	bucketID, err := strconv.ParseInt(c.Param("bucketId"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{Error: "invalid bucketId"})
		return
	}
	if err := h.store.DepotDeleteByBucket(space, int32(bucketID)); err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	c.Status(http.StatusNoContent)
}

func (h *depotHandler) getAllByBucket(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
		return
	}
	bucketID, err := strconv.ParseInt(c.Param("bucketId"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, protocol.ErrorResponse{Error: "invalid bucketId"})
		return
	}
	values, err := h.store.DepotGetAllByBucket(space, int32(bucketID))
	if err != nil {
		c.JSON(http.StatusInternalServerError, protocol.ErrorResponseFrom(err))
		return
	}
	incrementObjects(c, "depot", "read", len(values))
	c.JSON(http.StatusOK, protocol.DepotGetAllByBucketResponse{Values: values})
}
