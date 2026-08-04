package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects server metrics that can be added without changing Server.
// Counter values are always numbers; highlights are stored as display strings.
type Metrics struct {
	counters   sync.Map // map[string]*atomic.Uint64
	highlights sync.Map // map[string]string

	appsMu      sync.Mutex
	appCounters map[string]*counterSet
	otherApps   counterSet
}

const MaxApps = 100

type counterSet struct {
	counters sync.Map // map[string]*atomic.Uint64
}

func NewMetrics() *Metrics {
	m := &Metrics{appCounters: make(map[string]*counterSet)}
	m.SetHighlight("server_started_at", time.Now().UTC().Format(time.RFC3339))
	return m
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

// SetHighlight stores a display value for key. Values are converted to strings
// so callers can conveniently pass values such as room counts as well as text.
func (m *Metrics) SetHighlight(key string, value any) {
	m.highlights.Store(key, fmt.Sprint(value))
}

// SetHiglight is retained as an alias for the originally suggested spelling.
// New code should use SetHighlight.
func (m *Metrics) SetHiglight(key string, value any) {
	m.SetHighlight(key, value)
}

// Values returns a point-in-time snapshot suitable for JSON encoding. Counter
// and highlight names share the same namespace; a highlight takes precedence
// if the same name is used by both collections.
func (m *Metrics) Values() map[string]any {
	values := make(map[string]any)
	m.counters.Range(func(key, value any) bool {
		values[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	m.highlights.Range(func(key, value any) bool {
		values[key.(string)] = value.(string)
		return true
	})
	return values
}

// Snapshot returns separate counter and highlight collections for JSON APIs.
// It is a point-in-time view; counters may continue changing after it returns.
func (m *Metrics) Snapshot() Snapshot {
	snapshot := Snapshot{
		Counters:   make(map[string]uint64),
		Highlights: make(map[string]string),
		Apps:       make(map[string]map[string]uint64),
	}
	m.counters.Range(func(key, value any) bool {
		snapshot.Counters[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	m.highlights.Range(func(key, value any) bool {
		snapshot.Highlights[key.(string)] = value.(string)
		return true
	})
	m.appsMu.Lock()
	for appID, counters := range m.appCounters {
		snapshot.Apps[appID] = counters.snapshot()
	}
	if other := m.otherApps.snapshot(); len(other) > 0 {
		snapshot.Apps["other"] = other
	}
	m.appsMu.Unlock()
	return snapshot
}

func (c *counterSet) snapshot() map[string]uint64 {
	snapshot := make(map[string]uint64)
	c.counters.Range(func(key, value any) bool {
		snapshot[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return snapshot
}

// Snapshot is the response shape for the metrics endpoint.
type Snapshot struct {
	Counters   map[string]uint64            `json:"counters"`
	Highlights map[string]string            `json:"highlights"`
	Apps       map[string]map[string]uint64 `json:"apps"`
}
