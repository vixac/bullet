package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/vixac/bullet/metrics"
)

func TestMetricsEndpointReportsRouteAndObjectCounters(t *testing.T) {
	engine := gin.New()
	m := metrics.NewMetrics()
	SetupMetricsRouter(m, engine)
	engine.GET("/example", func(c *gin.Context) {
		incrementObjects(c, "track", "written", 2)
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/example", nil)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)

	request = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response = httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)

	var snapshot metrics.Snapshot
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &snapshot))
	require.Equal(t, uint64(1), snapshot.Counters["requests.GET /example"])
	require.Equal(t, uint64(2), snapshot.Counters["track.objects_written"])
	require.NotEmpty(t, snapshot.Highlights["server_started_at"])
}
