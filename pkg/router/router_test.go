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

package router_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/rakyll/miniate/pkg/router"
	"github.com/rakyll/miniate/pkg/runtime"
	"github.com/rakyll/miniate/pkg/store"
)

func TestRouterAutoResumeAndRouting(t *testing.T) {
	st, err := store.NewStore("", false)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}

	eng, err := runtime.NewEngine(st, runtime.EngineConfig{
		WorkerCount:    4,
		BaseWorkerPort: 9100,
	})
	if err != nil {
		t.Fatalf("failed to init engine: %v", err)
	}

	// Create a suspended actor
	_, _ = st.CreateActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "default",
			Name:     "my-counter",
		},
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: "default",
			Name:     "counter",
		},
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	r := router.NewRouter(st, eng, addr)
	go func() {
		_ = r.Start()
	}()
	defer func() {
		_ = r.Shutdown(context.Background())
	}()

	time.Sleep(100 * time.Millisecond)

	// Send request to suspended actor -> should auto-resume!
	req, err := http.NewRequest("POST", "http://"+addr+"/", nil)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("ate-target-actor", "default/my-counter")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	autoResumed := resp.Header.Get("X-Miniate-Auto-Resumed")
	if autoResumed != "true" {
		t.Errorf("expected X-Miniate-Auto-Resumed: true, got %q", autoResumed)
	}

	body, _ := io.ReadAll(resp.Body)
	t.Logf("Response body: %s", string(body))

	// Verify actor is now RUNNING in store
	act, _ := st.GetActor("default", "my-counter")
	if act.Status.State != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("expected actor to be RUNNING after auto-resume, got %s", act.Status.State)
	}
}
