package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/vixac/bullet/model"
	"github.com/vixac/bullet/store/store_interface"
)

type ledgerHandler struct{ store store_interface.LedgerStore }

func SetupLedgerRouter(store store_interface.LedgerStore, prefix string, engine *gin.Engine) *gin.Engine {
	h := &ledgerHandler{store: store}
	g := engine.Group(prefix)
	g.POST("/:ledgerId/entries", h.appendOne)
	g.POST("/:ledgerId/entries/batch", h.appendMany)
	g.POST("/read/backward", h.readBackward)
	g.POST("/read/forward", h.readForward)
	g.DELETE("/:ledgerId", h.deleteLedger)
	return engine
}

func ledgerSelector(req model.LedgerSelectorRequest) store_interface.LedgerSelector {
	ids := make([]store_interface.LedgerID, len(req.LedgerIDs))
	for i, id := range req.LedgerIDs {
		ids[i] = store_interface.LedgerID(id)
	}
	return store_interface.LedgerSelector{All: req.All, LedgerIDs: ids}
}

func ledgerRecordResponse(record store_interface.LedgerRecord) model.LedgerRecordResponse {
	return model.LedgerRecordResponse{LedgerID: string(record.LedgerID), Position: strconv.FormatInt(int64(record.Position), 10), AppendID: string(record.AppendID), CreatedAt: record.CreatedAt, Payload: record.Payload}
}

func ledgerRecordsResponse(records []store_interface.LedgerRecord) []model.LedgerRecordResponse {
	result := make([]model.LedgerRecordResponse, len(records))
	for i, record := range records {
		result[i] = ledgerRecordResponse(record)
	}
	return result
}

func (h *ledgerHandler) appendOne(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.LedgerAppendRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	record, err := h.store.LedgerAppend(space, store_interface.LedgerID(c.Param("ledgerId")), store_interface.LedgerAppendID(req.AppendID), req.Payload)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "ledger", "written", 1)
	c.JSON(http.StatusCreated, ledgerRecordResponse(record))
}

func (h *ledgerHandler) appendMany(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.LedgerAppendManyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	items := make([]store_interface.LedgerAppendItem, len(req.Items))
	for i, item := range req.Items {
		items[i] = store_interface.LedgerAppendItem{AppendID: store_interface.LedgerAppendID(item.AppendID), Payload: item.Payload}
	}
	records, err := h.store.LedgerAppendMany(space, store_interface.LedgerID(c.Param("ledgerId")), items)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "ledger", "written", len(records))
	c.JSON(http.StatusCreated, model.LedgerAppendManyResponse{Records: ledgerRecordsResponse(records)})
}

func (h *ledgerHandler) readBackward(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.LedgerReadBackwardRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	page, err := h.store.LedgerReadBackward(space, ledgerSelector(req.LedgerSelectorRequest), req.Cursor, req.Limit)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "ledger", "read", len(page.Records))
	c.JSON(http.StatusOK, model.LedgerPageResponse{Records: ledgerRecordsResponse(page.Records), NextCursor: page.NextCursor})
}

func parseLedgerPosition(value string) (store_interface.LedgerPosition, error) {
	if value == "" {
		return 0, nil
	}
	position, err := strconv.ParseInt(value, 10, 64)
	return store_interface.LedgerPosition(position), err
}

func (h *ledgerHandler) readForward(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	var req model.LedgerReadForwardRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	after, err := parseLedgerPosition(req.AfterPosition)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid after_position"})
		return
	}
	var through *store_interface.LedgerPosition
	if req.ThroughPosition != nil {
		parsed, err := parseLedgerPosition(*req.ThroughPosition)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid through_position"})
			return
		}
		through = &parsed
	}
	records, err := h.store.LedgerReadForward(space, ledgerSelector(req.LedgerSelectorRequest), after, through, req.Limit)
	if err != nil {
		respondError(c, err)
		return
	}
	incrementObjects(c, "ledger", "read", len(records))
	c.JSON(http.StatusOK, model.LedgerReadForwardResponse{Records: ledgerRecordsResponse(records)})
}

func (h *ledgerHandler) deleteLedger(c *gin.Context) {
	space, err := extractSpace(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	if err := h.store.LedgerDelete(space, store_interface.LedgerID(c.Param("ledgerId"))); err != nil {
		respondError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
