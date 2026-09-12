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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

type AgentState struct {
	Atespace  string    `json:"atespace"`
	ActorName string    `json:"actor"`
	Count     int64     `json:"count"`
	StartTime time.Time `json:"start_time"`
	Hostname  string    `json:"hostname"`
}

func main() {
	port := flag.Int("port", 8080, "HTTP server port")
	atespace := flag.String("atespace", os.Getenv("ATE_ATESPACE"), "Atespace name")
	actorName := flag.String("actor", os.Getenv("ATE_ACTOR"), "Actor name")
	flag.Parse()

	if *atespace == "" {
		*atespace = "demo"
	}
	if *actorName == "" {
		*actorName = "counter-1"
	}

	hostname, _ := os.Hostname()
	var counter int64
	startTime := time.Now()

	mux := http.NewServeMux()

	// Readiness probe
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	// State handler
	mux.HandleFunc("/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(AgentState{
			Atespace:  *atespace,
			ActorName: *actorName,
			Count:     atomic.LoadInt64(&counter),
			StartTime: startTime,
			Hostname:  hostname,
		})
	})

	// Default request handler (increments counter)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		newVal := atomic.AddInt64(&counter, 1)
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"message":   "Hello from Substrate Counter Agent!",
			"atespace":  *atespace,
			"actor":     *actorName,
			"count":     newVal,
			"path":      r.URL.Path,
			"method":    r.Method,
			"timestamp": time.Now().Format(time.RFC3339),
			"uptime":    time.Since(startTime).String(),
		}
		json.NewEncoder(w).Encode(resp)
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Starting Counter Agent on %s (atespace=%s, actor=%s)...", addr, *atespace, *actorName)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
