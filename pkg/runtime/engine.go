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

package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/rakyll/miniate/pkg/store"
)

var (
	ErrNoWorkersAvailable = errors.New("no free workers available in pool")
	ErrActorNotRunning    = errors.New("actor is not in running state")
)

type EngineConfig struct {
	WorkerCount    int
	BaseWorkerPort int
}

type Engine struct {
	mu           sync.Mutex
	st           *store.Store
	cfg          EngineConfig
	workerStatus map[string]*WorkerRuntimeInfo
}

type WorkerRuntimeInfo struct {
	Name          string
	Port          int
	AssignedActor string
	LastActive    time.Time
}

func NewEngine(st *store.Store, cfg EngineConfig) (*Engine, error) {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 8
	}
	if cfg.BaseWorkerPort <= 0 {
		cfg.BaseWorkerPort = 9100
	}

	eng := &Engine{
		st:           st,
		cfg:          cfg,
		workerStatus: make(map[string]*WorkerRuntimeInfo),
	}

	// Bootstrap workers in store if not present
	for i := 0; i < cfg.WorkerCount; i++ {
		wName := fmt.Sprintf("worker-%d", i)
		port := cfg.BaseWorkerPort + i

		eng.workerStatus[wName] = &WorkerRuntimeInfo{
			Name:       wName,
			Port:       port,
			LastActive: time.Now(),
		}

		if _, err := st.GetWorker(wName); err != nil {
			_, _ = st.CreateWorker(&ateapipb.Worker{
				Metadata: &ateapipb.ResourceMetadata{
					Name: wName,
				},
				Labels: map[string]string{
					"topology.kubernetes.io/zone":      "local-zone-1",
					"node.kubernetes.io/instance-type": "local.miniate",
				},
				WorkerNamespace: "default",
				WorkerPool:      "default",
				WorkerPod:       wName,
				Ip:              fmt.Sprintf("127.0.0.1:%d", port),
				Status: &ateapipb.WorkerStatus{
					State: ateapipb.WorkerState_WORKER_STATE_ACTIVE,
					Capacity: &ateapipb.WorkerResources{
						Actors: 1,
					},
					Allocated: &ateapipb.WorkerResources{
						Actors: 0,
					},
				},
			})
		}
	}

	return eng, nil
}

func (e *Engine) ResumeActor(ctx context.Context, atespace, name string) (*ateapipb.Actor, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	actor, err := e.st.GetActor(atespace, name)
	if err != nil {
		return nil, err
	}

	if actor.Status != nil && actor.Status.State == ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return actor, nil
	}

	// Find an idle worker
	workers, err := e.st.ListWorkers()
	if err != nil {
		return nil, err
	}

	var chosenWorker *ateapipb.Worker
	for _, w := range workers {
		if w.Status != nil && w.Status.State == ateapipb.WorkerState_WORKER_STATE_ACTIVE {
			allocatedActors := int32(0)
			if w.Status.Allocated != nil {
				allocatedActors = w.Status.Allocated.Actors
			}
			capacityActors := int32(1)
			if w.Status.Capacity != nil && w.Status.Capacity.Actors > 0 {
				capacityActors = w.Status.Capacity.Actors
			}

			if allocatedActors < capacityActors {
				chosenWorker = w
				break
			}
		}
	}

	if chosenWorker == nil {
		return nil, ErrNoWorkersAvailable
	}

	// Assign worker
	tmplName := ""
	if actor.ActorTemplate != nil {
		tmplName = actor.ActorTemplate.Name
	}
	actorRef := fmt.Sprintf("%s/%s/%s", atespace, tmplName, name)

	if chosenWorker.Status.Allocated == nil {
		chosenWorker.Status.Allocated = &ateapipb.WorkerResources{}
	}
	chosenWorker.Status.Allocated.Actors++
	if _, err := e.st.UpdateWorker(chosenWorker); err != nil {
		return nil, fmt.Errorf("failed to update worker assignment: %w", err)
	}

	workerPort := 9100
	if rInfo, ok := e.workerStatus[chosenWorker.Metadata.Name]; ok {
		workerPort = rInfo.Port
		rInfo.AssignedActor = actorRef
		rInfo.LastActive = time.Now()
	}

	workerIP := fmt.Sprintf("127.0.0.1:%d", workerPort)
	actor, err = e.st.SetActorState(atespace, name, ateapipb.ActorState_ACTOR_STATE_RUNNING, chosenWorker.Metadata.Name, workerIP)
	if err != nil {
		return nil, err
	}

	// Check if we have state to restore from snapshot
	if actor.Status != nil && actor.Status.ExternalSnapshot != nil && actor.Status.ExternalSnapshot.SnapshotUri != "" {
		snapURI := actor.Status.ExternalSnapshot.SnapshotUri
		if snap, ok := e.st.GetSnapshot(snapURI); ok {
			e.st.SetActorStateData(atespace, name, snap.Data)
			e.st.AppendLog(atespace, name, fmt.Sprintf("[%s] Resumed on %s (%s) from snapshot %s (version %d)", time.Now().Format(time.RFC3339), chosenWorker.Metadata.Name, workerIP, snapURI, snap.Version))
		} else {
			e.st.AppendLog(atespace, name, fmt.Sprintf("[%s] Resumed on %s (%s)", time.Now().Format(time.RFC3339), chosenWorker.Metadata.Name, workerIP))
		}
	} else {
		e.st.AppendLog(atespace, name, fmt.Sprintf("[%s] Resumed on %s (%s) fresh instance", time.Now().Format(time.RFC3339), chosenWorker.Metadata.Name, workerIP))
	}

	return actor, nil
}

func (e *Engine) SuspendActor(ctx context.Context, atespace, name string) (*ateapipb.Actor, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	actor, err := e.st.GetActor(atespace, name)
	if err != nil {
		return nil, "", err
	}

	if actor.Status != nil && actor.Status.State == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		snap := ""
		if actor.Status.ExternalSnapshot != nil {
			snap = actor.Status.ExternalSnapshot.SnapshotUri
		}
		return actor, snap, nil
	}

	prevWorkerPod := ""
	if actor.Status != nil && actor.Status.WorkerAssignment != nil {
		prevWorkerPod = actor.Status.WorkerAssignment.WorkerPod
	}

	if prevWorkerPod != "" {
		if w, err := e.st.GetWorker(prevWorkerPod); err == nil {
			if w.Status != nil && w.Status.Allocated != nil && w.Status.Allocated.Actors > 0 {
				w.Status.Allocated.Actors--
			}
			_, _ = e.st.UpdateWorker(w)
		}
		if rInfo, ok := e.workerStatus[prevWorkerPod]; ok {
			rInfo.AssignedActor = ""
		}
	}

	version := int64(1)
	if actor.Metadata != nil {
		version = actor.Metadata.Version
	}

	// Create snapshot
	snapURI := fmt.Sprintf("miniate-snap://%s/%s/v%d", atespace, name, version)
	stateData := e.st.GetActorStateData(atespace, name)
	e.st.SaveSnapshot(&store.SnapshotData{
		URI:       snapURI,
		Atespace:  atespace,
		ActorName: name,
		Version:   version,
		CreatedAt: time.Now(),
		Data:      stateData,
	})

	actor, err = e.st.SetActorState(atespace, name, ateapipb.ActorState_ACTOR_STATE_SUSPENDED, "", "")
	if err != nil {
		return nil, "", err
	}

	if actor.Status == nil {
		actor.Status = &ateapipb.ActorStatus{}
	}
	actor.Status.ExternalSnapshot = &ateapipb.ExternalSnapshot{
		SnapshotUri: snapURI,
	}

	e.st.AppendLog(atespace, name, fmt.Sprintf("[%s] Suspended to snapshot %s (freed worker %s)", time.Now().Format(time.RFC3339), snapURI, prevWorkerPod))

	return actor, snapURI, nil
}

func (e *Engine) PauseActor(ctx context.Context, atespace, name string) (*ateapipb.Actor, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	actor, err := e.st.GetActor(atespace, name)
	if err != nil {
		return nil, err
	}

	workerPod := ""
	workerIP := ""
	if actor.Status != nil && actor.Status.WorkerAssignment != nil {
		workerPod = actor.Status.WorkerAssignment.WorkerPod
		workerIP = actor.Status.WorkerAssignment.WorkerPodIp
	}

	actor, err = e.st.SetActorState(atespace, name, ateapipb.ActorState_ACTOR_STATE_PAUSED, workerPod, workerIP)
	if err != nil {
		return nil, err
	}

	e.st.AppendLog(atespace, name, fmt.Sprintf("[%s] Actor paused on node", time.Now().Format(time.RFC3339)))
	return actor, nil
}

func (e *Engine) HandleActorRequest(ctx context.Context, atespace, name, method, path string, body []byte) (int, map[string]string, []byte, error) {
	actor, err := e.st.GetActor(atespace, name)
	if err != nil {
		return 404, nil, []byte(fmt.Sprintf("actor %s/%s not found", atespace, name)), err
	}

	curState := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	workerPod := ""
	if actor.Status != nil {
		curState = actor.Status.State
		if actor.Status.WorkerAssignment != nil {
			workerPod = actor.Status.WorkerAssignment.WorkerPod
		}
	}

	if curState != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		return 503, nil, []byte(fmt.Sprintf("actor %s/%s is not running (state=%s)", atespace, name, curState)), ErrActorNotRunning
	}

	tmplName := ""
	if actor.ActorTemplate != nil {
		tmplName = actor.ActorTemplate.Name
	}

	state := e.st.GetActorStateData(atespace, name)
	currentCount := int64(0)
	if c, ok := state["counter"]; ok {
		switch v := c.(type) {
		case float64:
			currentCount = int64(v)
		case int64:
			currentCount = v
		case int:
			currentCount = int64(v)
		}
	}

	if method == "POST" || method == "PUT" {
		currentCount++
		state["counter"] = currentCount
		e.st.SetActorStateData(atespace, name, state)
	}

	version := int64(1)
	if actor.Metadata != nil {
		version = actor.Metadata.Version
	}

	respBody := fmt.Sprintf(`{"atespace":%q,"actor":%q,"template":%q,"worker":%q,"count":%d,"version":%d,"timestamp":%q}`+"\n",
		atespace, name, tmplName, workerPod, currentCount, version, time.Now().Format(time.RFC3339))

	e.st.AppendLog(atespace, name, fmt.Sprintf("[%s] HTTP %s %s -> status 200 (counter=%d, worker=%s)",
		time.Now().Format(time.RFC3339), method, path, currentCount, workerPod))

	headers := map[string]string{
		"Content-Type":     "application/json",
		"X-Miniate-Actor":  fmt.Sprintf("%s/%s", atespace, name),
		"X-Miniate-Worker": workerPod,
		"X-Miniate-State":  curState.String(),
	}

	return 200, headers, []byte(respBody), nil
}
