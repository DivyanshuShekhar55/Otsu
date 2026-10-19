// Package ring decides WHICH nodes store a key, using consistent hashing.
//
// Picture a circle of numbers from 0 to 2^64-1. Every node is placed on the
// circle many times ("virtual nodes", so load spreads evenly). To find the
// owners of a key we hash the key onto the same circle and walk clockwise,
// collecting distinct nodes. Those nodes are the key's "preference list".
//
// The big win of this idea: adding or removing a node only moves the keys
// next to it on the circle, not everything.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

// DefaultVNodes is how many points each node gets on the circle when the
// caller doesn't say. More points = more even spread (a bit more memory).
const DefaultVNodes = 128

// Node is one member of the cluster.
type Node struct {
	ID   string `json:"id"`   // unique name, e.g. "n1"
	Addr string `json:"addr"` // where to reach it, e.g. "127.0.0.1:7001"
}

// point is one virtual node: a spot on the circle that belongs to a real node.
type point struct {
	hash   uint64
	nodeID string
}

// Ring maps keys to nodes. It is built once and never changes afterwards
// (static membership), so it is safe to use from many goroutines.
type Ring struct {
	nodes  map[string]Node // real nodes by id
	points []point         // all virtual nodes, sorted by hash
}

// New builds a ring. vnodes <= 0 means DefaultVNodes.
func New(nodes []Node, vnodes int) (*Ring, error) {
	if len(nodes) == 0 {
		return nil, errors.New("ring: need at least one node")
	}
	if vnodes <= 0 {
		vnodes = DefaultVNodes
	}

	r := &Ring{nodes: make(map[string]Node, len(nodes))}

	for _, n := range nodes {
		if n.ID == "" {
			return nil, errors.New("ring: node with empty id")
		}
		if _, dup := r.nodes[n.ID]; dup {
			return nil, fmt.Errorf("ring: duplicate node id %q", n.ID)
		}
		r.nodes[n.ID] = n

		// Give this node `vnodes` spots on the circle: "n1#0", "n1#1", ...
		for i := 0; i < vnodes; i++ {
			r.points = append(r.points, point{
				hash:   hashOf(fmt.Sprintf("%s#%d", n.ID, i)),
				nodeID: n.ID,
			})
		}
	}

	// Sort around the circle. Ties are broken by node id so the result does
	// not depend on the order the caller listed the nodes in.
	sort.Slice(r.points, func(i, j int) bool {
		if r.points[i].hash != r.points[j].hash {
			return r.points[i].hash < r.points[j].hash
		}
		return r.points[i].nodeID < r.points[j].nodeID
	})

	return r, nil
}

// hashOf turns any string into a position on the circle.
// SHA-256 is slower than needed but spreads values very evenly, and its
// output never changes between Go versions or machines.
func hashOf(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8]) // first 8 bytes are plenty
}

// Replicas returns the preference list for key: the first n DISTINCT nodes
// found walking clockwise from the key's position. The first node in the
// list is the key's "primary".
//
// If n is bigger than the cluster, you get every node (once each).
func (r *Ring) Replicas(key string, n int) []Node {
	if n <= 0 {
		return nil
	}
	if n > len(r.nodes) {
		n = len(r.nodes)
	}

	// Find the first point at or after the key's hash...
	h := hashOf(key)
	start := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})

	// ...then walk clockwise, wrapping around the end, skipping nodes we
	// already picked (a node owns many points, we only want it once).
	result := make([]Node, 0, n)
	seen := make(map[string]bool, n)
	for i := 0; len(result) < n && i < len(r.points); i++ {
		p := r.points[(start+i)%len(r.points)]
		if seen[p.nodeID] {
			continue
		}
		seen[p.nodeID] = true
		result = append(result, r.nodes[p.nodeID])
	}
	return result
}

// Nodes returns every node in the cluster, sorted by id.
func (r *Ring) Nodes() []Node {
	out := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
