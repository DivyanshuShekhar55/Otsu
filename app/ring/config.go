package ring

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config is the static cluster description read from a JSON file.
// There is no discovery and no gossip: to change the cluster you edit the
// file and restart the nodes.
//
//	{
//	  "vnodes": 128,
//	  "nodes": [
//	    {"id": "n1", "addr": "127.0.0.1:7001"},
//	    {"id": "n2", "addr": "127.0.0.1:7002"},
//	    {"id": "n3", "addr": "127.0.0.1:7003"}
//	  ]
//	}
type Config struct {
	Nodes  []Node `json:"nodes"`
	VNodes int    `json:"vnodes"` // optional, 0 = DefaultVNodes
}

// LoadConfig reads and checks a config file.
func LoadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("ring: bad config %s: %w", path, err)
	}

	// Catch obvious mistakes here, with a clear message.
	for i, n := range cfg.Nodes {
		if n.ID == "" || n.Addr == "" {
			return Config{}, fmt.Errorf("ring: config %s: node #%d needs both id and addr", path, i)
		}
	}
	return cfg, nil
}

// NewRing builds the ring described by the config.
func (c Config) NewRing() (*Ring, error) {
	return New(c.Nodes, c.VNodes)
}
