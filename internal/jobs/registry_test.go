package jobs

import (
	"testing"
)

func TestRegistry_Register_Get(t *testing.T) {
	r := NewRegistry()
	r.Register(Hello{})
	if h := r.Get(HelloType); h == nil {
		t.Fatal("Get(HelloType) = nil")
	}
	if r.Get("nonexistent") != nil {
		t.Error("Get(nonexistent) should be nil")
	}
}

func TestRegistry_Types(t *testing.T) {
	r := NewRegistry()
	r.Register(Hello{})
	types := r.Types()
	if len(types) != 1 || types[0] != HelloType {
		t.Errorf("Types() = %v", types)
	}
}
