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

package store_test

import (
	"os"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/rakyll/miniate/pkg/store"
)

func TestStoreAtespace(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "miniate-store-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	st, err := store.NewStore(tempDir, true)
	if err != nil {
		t.Fatalf("failed to init store: %v", err)
	}

	// 1. Create Atespace
	as, err := st.CreateAtespace("test-space")
	if err != nil {
		t.Fatalf("failed to create atespace: %v", err)
	}
	if as.Metadata.Name != "test-space" {
		t.Errorf("expected atespace name test-space, got %s", as.Metadata.Name)
	}

	// 2. Duplicate Create should fail
	_, err = st.CreateAtespace("test-space")
	if err == nil {
		t.Errorf("expected error on duplicate create, got nil")
	}

	// 3. Get Atespace
	got, err := st.GetAtespace("test-space")
	if err != nil {
		t.Fatalf("failed to get atespace: %v", err)
	}
	if got.Metadata.Name != "test-space" {
		t.Errorf("expected test-space, got %s", got.Metadata.Name)
	}

	// 4. List Atespaces
	list, err := st.ListAtespaces()
	if err != nil {
		t.Fatalf("failed to list atespaces: %v", err)
	}
	// "default" and "test-space"
	if len(list) < 2 {
		t.Errorf("expected at least 2 atespaces, got %d", len(list))
	}

	// 5. Delete Atespace
	deleted, err := st.DeleteAtespace("test-space")
	if err != nil {
		t.Fatalf("failed to delete atespace: %v", err)
	}
	if deleted.Metadata.Name != "test-space" {
		t.Errorf("expected test-space deleted, got %s", deleted.Metadata.Name)
	}
}

func TestStoreActorLifecycle(t *testing.T) {
	tempDir, _ := os.MkdirTemp("", "miniate-actor-test-*")
	defer os.RemoveAll(tempDir)

	st, _ := store.NewStore(tempDir, false)
	_, _ = st.CreateAtespace("dev")

	// Create Template
	tmpl, err := st.CreateActorTemplate(&ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "dev",
			Name:     "counter-tmpl",
		},
	})
	if err != nil {
		t.Fatalf("failed to create template: %v", err)
	}
	if tmpl.Metadata.Name != "counter-tmpl" {
		t.Errorf("expected counter-tmpl, got %s", tmpl.Metadata.Name)
	}

	// Create Actor
	act, err := st.CreateActor(&ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: "dev",
			Name:     "counter-1",
		},
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: "dev",
			Name:     "counter-tmpl",
		},
	})
	if err != nil {
		t.Fatalf("failed to create actor: %v", err)
	}
	if act.Status.State != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("expected initial state SUSPENDED, got %s", act.Status.State)
	}

	// Set State
	act, err = st.SetActorState("dev", "counter-1", ateapipb.ActorState_ACTOR_STATE_RUNNING, "worker-0", "127.0.0.1:9100")
	if err != nil {
		t.Fatalf("failed to set state: %v", err)
	}
	if act.Status.State != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("expected RUNNING, got %s", act.Status.State)
	}
	if act.Status.WorkerAssignment.WorkerPod != "worker-0" {
		t.Errorf("expected worker-0, got %s", act.Status.WorkerAssignment.WorkerPod)
	}

	// Cannot delete running without any_state
	_, err = st.DeleteActor("dev", "counter-1", false)
	if err == nil {
		t.Errorf("expected error deleting running actor without any_state, got nil")
	}

	// Can delete with any_state
	_, err = st.DeleteActor("dev", "counter-1", true)
	if err != nil {
		t.Fatalf("failed to delete actor with any_state: %v", err)
	}
}
