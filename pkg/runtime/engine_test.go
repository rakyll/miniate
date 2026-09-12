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

package runtime_test

import (
	"context"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/rakyll/miniate/pkg/runtime"
	"github.com/rakyll/miniate/pkg/store"
)

func TestEngineResumeSuspend(t *testing.T) {
	st, err := store.NewStore("", false)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}

	eng, err := runtime.NewEngine(st, runtime.EngineConfig{
		WorkerCount:    2,
		BaseWorkerPort: 9100,
	})
	if err != nil {
		t.Fatalf("failed to init engine: %v", err)
	}

	// Create actor in default atespace
	_, err = st.CreateActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "default",
			Name:     "actor-a",
		},
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: "default",
			Name:     "counter",
		},
	})
	if err != nil {
		t.Fatalf("failed to create actor: %v", err)
	}

	// 1. Resume actor
	ctx := context.Background()
	act, err := eng.ResumeActor(ctx, "default", "actor-a")
	if err != nil {
		t.Fatalf("failed to resume actor: %v", err)
	}
	if act.Status.State != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("expected state RUNNING, got %s", act.Status.State)
	}
	if act.Status.WorkerAssignment == nil || act.Status.WorkerAssignment.WorkerPod == "" {
		t.Errorf("expected worker assignment, got nil or empty")
	}

	// 2. Handle HTTP request while running (POST increments counter)
	status, _, body, err := eng.HandleActorRequest(ctx, "default", "actor-a", "POST", "/", []byte{})
	if err != nil {
		t.Fatalf("failed to handle actor request: %v", err)
	}
	if status != 200 {
		t.Errorf("expected status 200, got %d", status)
	}
	t.Logf("Actor response: %s", string(body))

	// 3. Suspend actor
	act, snapURI, err := eng.SuspendActor(ctx, "default", "actor-a")
	if err != nil {
		t.Fatalf("failed to suspend actor: %v", err)
	}
	if act.Status.State != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("expected state SUSPENDED, got %s", act.Status.State)
	}
	if snapURI == "" {
		t.Errorf("expected non-empty snapshot URI")
	}

	// 4. Resume again and verify state persistence
	act, err = eng.ResumeActor(ctx, "default", "actor-a")
	if err != nil {
		t.Fatalf("failed to resume actor after suspend: %v", err)
	}
	status, _, body, err = eng.HandleActorRequest(ctx, "default", "actor-a", "GET", "/", []byte{})
	if err != nil {
		t.Fatalf("failed to handle request: %v", err)
	}
	if status != 200 {
		t.Errorf("expected status 200, got %d", status)
	}
}
