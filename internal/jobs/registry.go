package jobs

import "sync"

// Registry maps job type names to handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry returns a new registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register adds a handler for its Type(). Panics if type is empty or already registered.
func (r *Registry) Register(h Handler) {
	if h == nil || h.Type() == "" {
		panic("jobs: nil or empty-type handler")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[h.Type()]; ok {
		panic("jobs: duplicate handler type " + h.Type())
	}
	r.handlers[h.Type()] = h
}

// Get returns the handler for the given type, or nil.
func (r *Registry) Get(typ string) Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.handlers[typ]
}

// Types returns all registered job type names.
func (r *Registry) Types() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		out = append(out, t)
	}
	return out
}
