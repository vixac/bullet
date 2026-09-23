package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/protocol"
	si "github.com/vixac/bullet/store/store_interface"
)

// SetupWarehouseRouter serves JSON blobs; byte values use standard base64 JSON encoding.
func SetupWarehouseRouter(store si.WarehouseStore, prefix string, engine *gin.Engine) *gin.Engine {
	engine.UseRawPath = true
	engine.UnescapePathValues = true
	g := engine.Group(prefix)
	g.POST("/blobs", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
			return
		}
		var req protocol.PutBlobRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
			return
		}
		blob, err := store.WarehousePut(c.Request.Context(), space, req.Model())
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "written", 1)
		c.JSON(http.StatusOK, protocol.BlobFromModel(blob))
	})
	g.GET("/blobs/:id", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
			return
		}
		blob, err := store.WarehouseGet(c.Request.Context(), space, model.BlobID(c.Param("id")))
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "read", 1)
		c.JSON(http.StatusOK, protocol.BlobFromModel(blob))
	})
	g.POST("/blobs/batch-get", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
			return
		}
		var req protocol.WarehouseGetManyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
			return
		}
		blobs, err := store.WarehouseGetMany(c.Request.Context(), space, req.IDs)
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "read", len(blobs))
		c.JSON(http.StatusOK, protocol.BlobsFromModel(blobs))
	})
	g.POST("/checkpoints", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
			return
		}
		var wire protocol.PutCheckpointRequest
		if err := c.ShouldBindJSON(&wire); err != nil {
			c.JSON(http.StatusBadRequest, protocol.ErrorResponseFrom(err))
			return
		}
		req, err := wire.Model()
		if err != nil {
			respondError(c, err)
			return
		}
		ref, err := store.WarehousePutCheckpoint(c.Request.Context(), space, req)
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "written", 1)
		c.JSON(http.StatusOK, protocol.CheckpointRefFromModel(ref))
	})
	g.GET("/checkpoint-sequences/:sequenceId/checkpoints/latest", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
			return
		}
		ref, err := store.WarehouseGetLatestCheckpoint(c.Request.Context(), space, model.CheckpointSequenceID(c.Param("sequenceId")))
		if err != nil {
			respondError(c, err)
			return
		}
		response := protocol.LatestCheckpointResponse{}
		if ref != nil {
			converted := protocol.CheckpointRefFromModel(*ref)
			response.Checkpoint = &converted
			incrementObjects(c, "warehouse", "read", 1)
		}
		c.JSON(http.StatusOK, response)
	})
	g.GET("/checkpoint-sequences/:sequenceId/checkpoints", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
			return
		}
		atOrBeforeValue, present := c.GetQuery("at_or_before")
		if !present {
			respondError(c, fmt.Errorf("%w: at_or_before is required", model.ErrCheckpointInvalid))
			return
		}
		atOrBefore, err := strconv.ParseInt(atOrBeforeValue, 10, 64)
		if err != nil || atOrBefore < 0 {
			respondError(c, fmt.Errorf("%w: invalid at_or_before", model.ErrCheckpointInvalid))
			return
		}
		limitValue, present := c.GetQuery("limit")
		if !present {
			respondError(c, fmt.Errorf("%w: limit is required", model.ErrCheckpointInvalid))
			return
		}
		limit, err := strconv.ParseInt(limitValue, 10, 0)
		if err != nil || limit < 0 {
			respondError(c, fmt.Errorf("%w: invalid limit", model.ErrCheckpointInvalid))
			return
		}
		refs, err := store.WarehouseFindCheckpoints(c.Request.Context(), space, model.CheckpointSequenceID(c.Param("sequenceId")), model.LedgerPosition(atOrBefore), int(limit))
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "read", len(refs))
		c.JSON(http.StatusOK, protocol.CheckpointCandidatesResponse{Checkpoints: protocol.CheckpointRefsFromModel(refs)})
	})
	g.GET("/checkpoints/:checkpointId", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
			return
		}
		checkpoint, err := store.WarehouseGetCheckpoint(c.Request.Context(), space, model.CheckpointID(c.Param("checkpointId")))
		if err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "read", 1)
		c.JSON(http.StatusOK, protocol.CheckpointFromModel(checkpoint))
	})
	g.POST("/checkpoints/:checkpointId/corrupt", func(c *gin.Context) {
		space, err := extractSpace(c)
		if err != nil {
			c.JSON(http.StatusUnauthorized, protocol.ErrorResponseFrom(err))
			return
		}
		if err := store.WarehouseMarkCheckpointCorrupt(c.Request.Context(), space, model.CheckpointID(c.Param("checkpointId"))); err != nil {
			respondError(c, err)
			return
		}
		incrementObjects(c, "warehouse", "written", 1)
		c.Status(http.StatusNoContent)
	})
	return engine
}
