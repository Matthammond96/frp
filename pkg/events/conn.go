package events

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// ConnEvent represents a proxy connection open/close event.
type ConnEvent struct {
    Action     string   `json:"action"` // open|close|register|unregister
    ProxyName  string   `json:"proxyName"`
    ProxyType  string   `json:"proxyType"`
    Timestamp  int64    `json:"ts"`
    Domains    []string `json:"domains,omitempty"`
    RemoteAddr string   `json:"remoteAddr,omitempty"`
    Group      string   `json:"group,omitempty"`
}

type hub struct {
    mu   sync.RWMutex
    subs map[chan ConnEvent]struct{}
}

var h = &hub{subs: make(map[chan ConnEvent]struct{})}

// PublishConnEvent publishes a connection event.
type PublishOptions struct {
    Domains    []string
    RemoteAddr string
    Group      string
}

func PublishConnEvent(action, name, pType string, opts ...PublishOptions) {
    ev := ConnEvent{Action: action, ProxyName: name, ProxyType: pType, Timestamp: time.Now().Unix()}
    if len(opts) > 0 {
        o := opts[0]
        if len(o.Domains) > 0 { ev.Domains = o.Domains }
        if o.RemoteAddr != "" { ev.RemoteAddr = o.RemoteAddr }
        if o.Group != "" { ev.Group = o.Group }
    }
    h.mu.RLock()
    for ch := range h.subs {
        select { case ch <- ev: default: }
    }
    h.mu.RUnlock()
}

// Subscribe returns a channel to receive connection events and an unsubscribe func.
func Subscribe() (chan ConnEvent, func()) {
    ch := make(chan ConnEvent, 64)
    h.mu.Lock(); h.subs[ch] = struct{}{}; h.mu.Unlock()
    unsubscribe := func() { h.mu.Lock(); delete(h.subs, ch); h.mu.Unlock(); close(ch) }
    return ch, unsubscribe
}

// ServeSSE streams connection events in Server-Sent Events format.
func ServeSSE(w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok { http.Error(w, "stream unsupported", http.StatusInternalServerError); return }
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")
    ch, unsubscribe := Subscribe()
    defer unsubscribe()
    _, _ = w.Write([]byte(": connected\n\n")); flusher.Flush()
    heartbeat := time.NewTicker(30 * time.Second)
    defer heartbeat.Stop()
    for {
        select {
        case <-r.Context().Done():
            return
        case ev := <-ch:
            b, _ := json.Marshal(ev)
            _, _ = w.Write([]byte("event: conn\n"))
            _, _ = w.Write([]byte("data: "))
            _, _ = w.Write(b)
            _, _ = w.Write([]byte("\n\n"))
            flusher.Flush()
        case <-heartbeat.C:
            _, _ = w.Write([]byte(": ping\n\n"))
            flusher.Flush()
        }
    }
}
