package scope

import (
	"encoding/json"
	"os"
	"testing"
)

func TestInScope_SharedContractVectors(t *testing.T) {
	raw, err := os.ReadFile("../../../../shared/contracts/url_canonicalization.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}

	var vectors struct {
		Scope []struct {
			Host      string `json:"host"`
			ScopeHost string `json:"scope_host"`
			InScope   bool   `json:"in_scope"`
		} `json:"scope"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}

	for _, v := range vectors.Scope {
		if got := InScope(v.Host, v.ScopeHost); got != v.InScope {
			t.Errorf("InScope(%q, %q) = %v, want %v", v.Host, v.ScopeHost, got, v.InScope)
		}
	}
}
