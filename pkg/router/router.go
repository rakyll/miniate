// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/rakyll/miniate/pkg/runtime"
	"github.com/rakyll/miniate/pkg/store"
)

type Router struct {
	st         *store.Store
	eng        *runtime.Engine
	listenAddr string
	server     *http.Server
}

func NewRouter(st *store.Store, eng *runtime.Engine, listenAddr string) *Router {
	r := &Router{
		st:         st,
		eng:        eng,
		listenAddr: listenAddr,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", r.handleHealth)
	mux.HandleFunc("/readyz", r.handleHealth)
	mux.HandleFunc("/info", r.handleInfo)
	mux.HandleFunc("/", r.handleRequest)

	r.server = &http.Server{
		Addr:    listenAddr,
		Handler: mux,
	}

	return r
}

func (r *Router) Start() error {
	return r.server.ListenAndServe()
}

func (r *Router) Shutdown(ctx context.Context) error {
	return r.server.Shutdown(ctx)
}

func (r *Router) handleHealth(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (r *Router) handleInfo(w http.ResponseWriter, req *http.Request) {
	workers, _ := r.st.ListWorkers()
	actors, _ := r.st.ListActors("")
	atespaces, _ := r.st.ListAtespaces()

	info := map[string]any{
		"system":    "Agent Substrate Local (Miniate)",
		"status":    "running",
		"workers":   len(workers),
		"actors":    len(actors),
		"atespaces": len(atespaces),
		"timestamp": time.Now().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(info)
}

func (r *Router) parseTargetActor(req *http.Request) (string, string) {
	// 1. Header: ate-target-actor: <atespace>/<actor>
	if target := req.Header.Get("ate-target-actor"); target != "" {
		parts := strings.SplitN(strings.TrimSpace(target), "/", 2)
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
		return "default", parts[0]
	}

	// 2. Query param: ?_actor=<atespace>/<actor>
	if target := req.URL.Query().Get("_actor"); target != "" {
		parts := strings.SplitN(strings.TrimSpace(target), "/", 2)
		if len(parts) == 2 {
			return parts[0], parts[1]
		}
		return "default", parts[0]
	}

	// 3. Subdomain in Host: <actor>.<atespace>.localhost
	host := req.Host
	if colon := strings.Index(host, ":"); colon != -1 {
		host = host[:colon]
	}
	subParts := strings.Split(host, ".")
	if len(subParts) >= 3 && (subParts[len(subParts)-1] == "localhost" || subParts[len(subParts)-1] == "local") {
		return subParts[1], subParts[0]
	}

	return "", ""
}

func (r *Router) handleRequest(w http.ResponseWriter, req *http.Request) {
	atespace, actorName := r.parseTargetActor(req)

	if atespace == "" || actorName == "" {
		if req.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`Miniate Router (atenet)
To route requests to an actor, provide the header:
  ate-target-actor: <atespace>/<actor-name>

Example:
  curl -X POST -H "ate-target-actor: default/my-actor" http://localhost:8000/
`))
			return
		}
		http.Error(w, "missing ate-target-actor header (format: <atespace>/<actor-name>)", http.StatusBadRequest)
		return
	}

	actor, err := r.st.GetActor(atespace, actorName)
	if err != nil {
		http.Error(w, fmt.Sprintf("actor %s/%s not found", atespace, actorName), http.StatusNotFound)
		return
	}

	curState := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	if actor.Status != nil {
		curState = actor.Status.State
	}

	// Auto-resume if actor is suspended or paused
	if curState == ateapipb.ActorState_ACTOR_STATE_SUSPENDED || curState == ateapipb.ActorState_ACTOR_STATE_PAUSED {
		startResume := time.Now()
		actor, err = r.eng.ResumeActor(req.Context(), atespace, actorName)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to auto-resume actor: %v", err), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("X-Miniate-Auto-Resumed", "true")
		w.Header().Set("X-Miniate-Resume-Duration", time.Since(startResume).String())
	}

	body, _ := io.ReadAll(req.Body)
	_ = req.Body.Close()

	statusCode, headers, respBody, err := r.eng.HandleActorRequest(req.Context(), atespace, actorName, req.Method, req.URL.Path, body)
	if err != nil {
		http.Error(w, fmt.Sprintf("error executing actor request: %v", err), statusCode)
		return
	}

	for k, v := range headers {
		w.Header().Set(k, v)
	}
	w.WriteHeader(statusCode)
	_, _ = w.Write(respBody)
}
