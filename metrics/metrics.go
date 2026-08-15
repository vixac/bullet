package metrics

import (
	"math"
	"sync"
	"sync/atomic"
)

// Metrics collects server metrics that can be added without changing Server.
// Counters are cumulative values; gauges are values that may move up or down.
type Metrics struct {
	counters sync.Map // map[string]*atomic.Uint64
	gauges   sync.Map // map[string]*atomic.Uint64 containing float64 bits

	appsMu      sync.Mutex
	appCounters map[string]*counterSet
	otherApps   counterSet
}

const MaxApps = 100

type counterSet struct {
	counters sync.Map // map[string]*atomic.Uint64
}

func NewMetrics() *Metrics {
	return &Metrics{appCounters: make(map[string]*counterSet)}
}

// IncrementCounter adds one to the counter identified by key, creating it on
// its first use. It is safe to call concurrently.
func (m *Metrics) IncrementCounter(key string) {
	m.AddCounter(key, 1)
}

// AddCounter adds delta to the counter identified by key, creating it on its
// first use. It is safe to call concurrently.
func (m *Metrics) AddCounter(key string, delta uint64) {
	addCounter(&m.counters, key, delta)
}

// AddAppCounter records a counter for one application. The first MaxApps
// distinct application IDs have individual counters; later applications are
// combined in the "other" bucket to bound memory usage.
func (m *Metrics) AddAppCounter(appID, key string, delta uint64) {
	if appID == "" {
		return
	}
	m.appCounterSet(appID).add(key, delta)
}

func (m *Metrics) appCounterSet(appID string) *counterSet {
	m.appsMu.Lock()
	defer m.appsMu.Unlock()
	if counters, ok := m.appCounters[appID]; ok {
		return counters
	}
	if len(m.appCounters) >= MaxApps {
		return &m.otherApps
	}
	counters := &counterSet{}
	m.appCounters[appID] = counters
	return counters
}

func (c *counterSet) add(key string, delta uint64) {
	addCounter(&c.counters, key, delta)
}

func addCounter(counters *sync.Map, key string, delta uint64) {
	value, _ := counters.LoadOrStore(key, &atomic.Uint64{})
	value.(*atomic.Uint64).Add(delta)
}

// SetGauge sets the current value of a gauge. It is safe to call concurrently.
func (m *Metrics) SetGauge(key string, value float64) {
	gauge, _ := m.gauges.LoadOrStore(key, &atomic.Uint64{})
	gauge.(*atomic.Uint64).Store(math.Float64bits(value))
}

// Snapshot returns the observations API response.
// It is a point-in-time view; counters may continue changing after it returns.
func (m *Metrics) Snapshot() Snapshot {
	bullet := Namespace{
		Counters: make(map[string]uint64),
		Gauges:   make(map[string]float64),
	}
	m.counters.Range(func(key, value any) bool {
		bullet.Counters[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	m.gauges.Range(func(key, value any) bool {
		bullet.Gauges[key.(string)] = math.Float64frombits(value.(*atomic.Uint64).Load())
		return true
	})
	return Snapshot{Namespaces: map[string]Namespace{"bullet": bullet}}
}

func (c *counterSet) snapshot() map[string]uint64 {
	snapshot := make(map[string]uint64)
	c.counters.Range(func(key, value any) bool {
		snapshot[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return snapshot
}

// Snapshot is the response shape for the observations endpoint.
type Snapshot struct {
	Namespaces map[string]Namespace `json:"namespaces"`
}

type Namespace struct {
	Counters map[string]uint64  `json:"counters"`
	Gauges   map[string]float64 `json:"gauges"`
}
