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

func TestObservationsEndpointReportsRouteAndObjectCounters(t *testing.T) {
	engine := gin.New()
	m := metrics.NewMetrics()
	SetupObservationsRouter(m, engine)
	engine.GET("/example", func(c *gin.Context) {
		incrementObjects(c, "track", "written", 2)
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/example", nil)
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	require.Equal(t, http.StatusNoContent, response.Code)

	request = httptest.NewRequest(http.MethodGet, "/observations", nil)
	response = httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)

	var snapshot metrics.Snapshot
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &snapshot))
	bullet := snapshot.Namespaces["bullet"]
	require.Equal(t, uint64(1), bullet.Counters["requests.GET /example"])
	require.Equal(t, uint64(2), bullet.Counters["track.objects_written"])
	require.Empty(t, bullet.Gauges)

	request = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response = httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	require.Equal(t, http.StatusNotFound, response.Code)
}
