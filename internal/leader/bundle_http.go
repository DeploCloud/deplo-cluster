package leader

import (
	"net/http"

	"github.com/DeploCloud/deplo-cluster/internal/bundles"
)

type bundleRef struct {
	Cluster string `json:"cluster"`
	Kind    string `json:"kind"`
	ID      string `json:"id"`
}

type putRequest struct {
	bundleRef
	DeploymentID string       `json:"deploymentId"`
	Sealed       string       `json:"sealed"`
	Meta         bundles.Meta `json:"meta"`
}

// bundleRoutes serves the key and the sealed bundles. Only the public key ever leaves: a bundle is opened to restart one workload.
func (l *Leader) bundleRoutes(mux *http.ServeMux) {
	store := func(w http.ResponseWriter) *bundles.Store {
		if l.cfg.Bundles == nil {
			http.Error(w, "this leader keeps no state directory, so it cannot hold bundles", http.StatusServiceUnavailable)
		}
		return l.cfg.Bundles
	}
	mux.HandleFunc("POST /call/leader.key", func(w http.ResponseWriter, r *http.Request) {
		if s := store(w); s != nil {
			reply(w, map[string]string{"publicKey": s.PublicKey()})
		}
	})
	mux.HandleFunc("POST /call/leader.bundle.put", func(w http.ResponseWriter, r *http.Request) {
		s := store(w)
		var req putRequest
		if s == nil || !decode(w, r, &req) {
			return
		}
		if err := s.PutBase64(req.Cluster, req.Kind, req.ID, req.DeploymentID, req.Sealed, req.Meta); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply(w, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /call/leader.bundle.drop", func(w http.ResponseWriter, r *http.Request) {
		s := store(w)
		var req bundleRef
		if s == nil || !decode(w, r, &req) {
			return
		}
		if err := s.Drop(req.Cluster, req.Kind, req.ID); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply(w, map[string]bool{"ok": true})
	})
	mux.HandleFunc("POST /call/leader.bundle.list", func(w http.ResponseWriter, r *http.Request) {
		s := store(w)
		var req bundleRef
		if s == nil || !decode(w, r, &req) {
			return
		}
		held, err := s.List(req.Cluster)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply(w, map[string]any{"bundles": held})
	})
}
