package ring

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// makeNodes returns n nodes named n0, n1, ...
func makeNodes(n int) []Node {
	nodes := make([]Node, n)
	for i := range nodes {
		nodes[i] = Node{ID: fmt.Sprintf("n%d", i), Addr: fmt.Sprintf("127.0.0.1:%d", 7000+i)}
	}
	return nodes
}

// Every key gets N different nodes (never the same node twice).
func TestReplicasAreDistinct(t *testing.T) {
	r, err := New(makeNodes(5), 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		list := r.Replicas(fmt.Sprintf("key-%d", i), 3)
		if len(list) != 3 {
			t.Fatalf("want 3 replicas, got %d", len(list))
		}
		seen := map[string]bool{}
		for _, n := range list {
			if seen[n.ID] {
				t.Fatalf("node %s appears twice in %v", n.ID, list)
			}
			seen[n.ID] = true
		}
	}
}

// Asking for more replicas than nodes just returns every node.
func TestReplicasMoreThanNodes(t *testing.T) {
	r, _ := New(makeNodes(3), 0)
	if got := len(r.Replicas("k", 5)); got != 3 {
		t.Fatalf("want 3 (all nodes), got %d", got)
	}
	if got := r.Replicas("k", 0); got != nil {
		t.Fatalf("n=0 should give nothing, got %v", got)
	}
}

// Same key, same answer, no matter in which order the nodes were listed.
func TestReplicasDeterministic(t *testing.T) {
	nodes := makeNodes(4)
	reversed := []Node{nodes[3], nodes[2], nodes[1], nodes[0]}

	a, _ := New(nodes, 0)
	b, _ := New(reversed, 0)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		la, lb := a.Replicas(key, 3), b.Replicas(key, 3)
		for j := range la {
			if la[j].ID != lb[j].ID {
				t.Fatalf("key %s: %v vs %v", key, la, lb)
			}
		}
	}
}

// Virtual nodes should spread keys roughly evenly.
func TestBalance(t *testing.T) {
	r, _ := New(makeNodes(3), 0)

	const total = 30000
	counts := map[string]int{}
	for i := 0; i < total; i++ {
		primary := r.Replicas(fmt.Sprintf("key-%d", i), 1)[0]
		counts[primary.ID]++
	}

	if len(counts) != 3 {
		t.Fatalf("only %d nodes received keys", len(counts))
	}
	for id, c := range counts {
		pct := c * 100 / total // fair share is about 33%
		if pct < 20 || pct > 47 {
			t.Fatalf("node %s owns %d%% of keys, that's too uneven", id, pct)
		}
	}
}

// The core promise of consistent hashing: when a node is added, a key either
// stays where it was or moves TO the new node. Nothing shuffles between the
// old nodes.
func TestAddingNodeMovesLittle(t *testing.T) {
	before, _ := New(makeNodes(3), 0) // n0 n1 n2
	after, _ := New(makeNodes(4), 0)  // n0 n1 n2 + n3

	const total = 10000
	moved := 0
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key-%d", i)
		was := before.Replicas(key, 1)[0].ID
		now := after.Replicas(key, 1)[0].ID
		if was != now {
			moved++
			if now != "n3" {
				t.Fatalf("key %s moved from %s to %s, but only moves to the new node are allowed", key, was, now)
			}
		}
	}

	// About 1/4 of the keys should move. Anything near "all" would be a bug.
	if moved == 0 || moved*100/total > 40 {
		t.Fatalf("%d of %d keys moved, expected roughly a quarter", moved, total)
	}
}

func TestNewRejectsBadInput(t *testing.T) {
	if _, err := New(nil, 0); err == nil {
		t.Fatal("empty node list should fail")
	}
	if _, err := New([]Node{{ID: "", Addr: "x"}}, 0); err == nil {
		t.Fatal("empty id should fail")
	}
	dup := []Node{{ID: "a", Addr: "x"}, {ID: "a", Addr: "y"}}
	if _, err := New(dup, 0); err == nil {
		t.Fatal("duplicate id should fail")
	}
}

// ---- config ----------------------------------------------------------------

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig(t *testing.T) {
	path := writeConfig(t, `{
		"vnodes": 16,
		"nodes": [
			{"id": "n1", "addr": "127.0.0.1:7001"},
			{"id": "n2", "addr": "127.0.0.1:7002"},
			{"id": "n3", "addr": "127.0.0.1:7003"}
		]
	}`)

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes) != 3 || cfg.VNodes != 16 || cfg.Nodes[1].Addr != "127.0.0.1:7002" {
		t.Fatalf("unexpected config: %+v", cfg)
	}

	r, err := cfg.NewRing()
	if err != nil {
		t.Fatal(err)
	}
	if got := len(r.Replicas("hello", 3)); got != 3 {
		t.Fatalf("want 3 replicas, got %d", got)
	}
}

func TestLoadConfigErrors(t *testing.T) {
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("missing file should fail")
	}
	if _, err := LoadConfig(writeConfig(t, `{not json`)); err == nil {
		t.Fatal("bad json should fail")
	}
	if _, err := LoadConfig(writeConfig(t, `{"nodes":[{"id":"n1"}]}`)); err == nil {
		t.Fatal("node without addr should fail")
	}
}
