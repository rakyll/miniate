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

package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrAlreadyExists   = errors.New("already exists")
	ErrPrecondition    = errors.New("failed precondition")
	ErrInvalidArgument = errors.New("invalid argument")
)

type Store struct {
	mu             sync.RWMutex
	stateDir       string
	persist        bool
	atespaces      map[string]*ateapipb.Atespace
	templates      map[string]*ateapipb.ActorTemplate // key: atespace/name
	actors         map[string]*ateapipb.Actor         // key: atespace/name
	workers        map[string]*ateapipb.Worker        // key: name
	tags           map[string]*ateapipb.Tag           // key: atespace/name
	egressPolicies map[string]*ateapipb.EgressPolicy  // key: atespace/actor
	logs           map[string][]string                // key: atespace/actor -> lines
	actorStateData map[string]map[string]any          // key: atespace/actor -> arbitrary actor state
	snapshots      map[string]*SnapshotData           // key: snapshotURI -> SnapshotData
}

type SnapshotData struct {
	URI       string
	Atespace  string
	ActorName string
	Version   int64
	CreatedAt time.Time
	Data      map[string]any
}

func NewStore(stateDir string, persist bool) (*Store, error) {
	s := &Store{
		stateDir:       stateDir,
		persist:        persist,
		atespaces:      make(map[string]*ateapipb.Atespace),
		templates:      make(map[string]*ateapipb.ActorTemplate),
		actors:         make(map[string]*ateapipb.Actor),
		workers:        make(map[string]*ateapipb.Worker),
		tags:           make(map[string]*ateapipb.Tag),
		egressPolicies: make(map[string]*ateapipb.EgressPolicy),
		logs:           make(map[string][]string),
		actorStateData: make(map[string]map[string]any),
		snapshots:      make(map[string]*SnapshotData),
	}

	if persist && stateDir != "" {
		if err := os.MkdirAll(stateDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create state dir: %w", err)
		}
		_ = s.loadFromDisk()
	}

	// Always ensure default atespace "default" exists
	if _, exists := s.atespaces["default"]; !exists {
		now := timestamppb.Now()
		s.atespaces["default"] = &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{
				Name:       "default",
				Uid:        uuid.NewString(),
				Version:    1,
				CreateTime: now,
				UpdateTime: now,
			},
		}
	}

	return s, nil
}

// ActorKey generates the canonical storage key for an actor or template.
func ActorKey(atespace, name string) string {
	return fmt.Sprintf("%s/%s", atespace, name)
}

// --- Atespace operations ---

func (s *Store) CreateAtespace(name string) (*ateapipb.Atespace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: atespace name cannot be empty", ErrInvalidArgument)
	}
	if _, exists := s.atespaces[name]; exists {
		return nil, fmt.Errorf("%w: atespace %q", ErrAlreadyExists, name)
	}

	now := timestamppb.Now()
	as := &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{
			Name:       name,
			Uid:        uuid.NewString(),
			Version:    1,
			CreateTime: now,
			UpdateTime: now,
		},
	}
	s.atespaces[name] = as
	s.saveToDiskLocked()
	return as, nil
}

func (s *Store) GetAtespace(name string) (*ateapipb.Atespace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	as, exists := s.atespaces[name]
	if !exists {
		return nil, fmt.Errorf("%w: atespace %q", ErrNotFound, name)
	}
	return as, nil
}

func (s *Store) ListAtespaces() ([]*ateapipb.Atespace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]*ateapipb.Atespace, 0, len(s.atespaces))
	for _, as := range s.atespaces {
		res = append(res, as)
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Metadata == nil || res[j].Metadata == nil {
			return false
		}
		return res[i].Metadata.Name < res[j].Metadata.Name
	})
	return res, nil
}

func (s *Store) DeleteAtespace(name string) (*ateapipb.Atespace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	as, exists := s.atespaces[name]
	if !exists {
		return nil, fmt.Errorf("%w: atespace %q", ErrNotFound, name)
	}

	for _, act := range s.actors {
		if act.Metadata != nil && act.Metadata.Atespace == name {
			return nil, fmt.Errorf("%w: atespace %q still contains actors", ErrPrecondition, name)
		}
	}
	for _, tag := range s.tags {
		if tag.Metadata != nil && tag.Metadata.Atespace == name {
			return nil, fmt.Errorf("%w: atespace %q still contains tags", ErrPrecondition, name)
		}
	}

	delete(s.atespaces, name)
	s.saveToDiskLocked()
	return as, nil
}

// --- Actor Template operations ---

func (s *Store) CreateActorTemplate(template *ateapipb.ActorTemplate) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if template == nil || template.Metadata == nil {
		return nil, fmt.Errorf("%w: template metadata is required", ErrInvalidArgument)
	}
	atespace := template.Metadata.Atespace
	name := template.Metadata.Name
	if atespace == "" || name == "" {
		return nil, fmt.Errorf("%w: atespace and name are required in template metadata", ErrInvalidArgument)
	}

	if _, ok := s.atespaces[atespace]; !ok {
		return nil, fmt.Errorf("%w: atespace %q does not exist", ErrPrecondition, atespace)
	}

	key := ActorKey(atespace, name)
	if _, exists := s.templates[key]; exists {
		return nil, fmt.Errorf("%w: template %q in atespace %q", ErrAlreadyExists, name, atespace)
	}

	now := timestamppb.Now()
	template.Metadata.Uid = uuid.NewString()
	template.Metadata.Version = 1
	template.Metadata.CreateTime = now
	template.Metadata.UpdateTime = now

	goldenSnapURI := fmt.Sprintf("miniate-snap://%s/golden-%s", atespace, name)
	if template.Status == nil {
		template.Status = &ateapipb.ActorTemplateStatus{}
	}
	template.Status.GoldenSnapshotStatus = &ateapipb.GoldenSnapshotStatus{
		GoldenSnapshot: &ateapipb.ExternalSnapshot{
			SnapshotUri: goldenSnapURI,
		},
	}

	s.templates[key] = template
	s.saveToDiskLocked()
	return template, nil
}

func (s *Store) GetActorTemplate(atespace, name string) (*ateapipb.ActorTemplate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := ActorKey(atespace, name)
	tmpl, exists := s.templates[key]
	if !exists {
		return nil, fmt.Errorf("%w: template %q in atespace %q", ErrNotFound, name, atespace)
	}
	return tmpl, nil
}

func (s *Store) ListActorTemplates(atespace string) ([]*ateapipb.ActorTemplate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]*ateapipb.ActorTemplate, 0)
	for _, tmpl := range s.templates {
		if tmpl.Metadata == nil {
			continue
		}
		if atespace == "" || tmpl.Metadata.Atespace == atespace {
			res = append(res, tmpl)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Metadata == nil || res[j].Metadata == nil {
			return false
		}
		if res[i].Metadata.Atespace != res[j].Metadata.Atespace {
			return res[i].Metadata.Atespace < res[j].Metadata.Atespace
		}
		return res[i].Metadata.Name < res[j].Metadata.Name
	})
	return res, nil
}

func (s *Store) DeleteActorTemplate(atespace, name string) (*ateapipb.ActorTemplate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ActorKey(atespace, name)
	tmpl, exists := s.templates[key]
	if !exists {
		return nil, fmt.Errorf("%w: template %q in atespace %q", ErrNotFound, name, atespace)
	}

	delete(s.templates, key)
	s.saveToDiskLocked()
	return tmpl, nil
}

// --- Actor operations ---

func (s *Store) CreateActor(actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if actor == nil || actor.Metadata == nil {
		return nil, fmt.Errorf("%w: actor metadata is required", ErrInvalidArgument)
	}
	atespace := actor.Metadata.Atespace
	name := actor.Metadata.Name
	if atespace == "" || name == "" {
		return nil, fmt.Errorf("%w: atespace and name are required", ErrInvalidArgument)
	}

	if _, ok := s.atespaces[atespace]; !ok {
		return nil, fmt.Errorf("%w: atespace %q does not exist", ErrPrecondition, atespace)
	}

	key := ActorKey(atespace, name)
	if _, exists := s.actors[key]; exists {
		return nil, fmt.Errorf("%w: actor %q in atespace %q", ErrAlreadyExists, name, atespace)
	}

	now := timestamppb.Now()
	actor.Metadata.Uid = uuid.NewString()
	actor.Metadata.Version = 1
	actor.Metadata.CreateTime = now
	actor.Metadata.UpdateTime = now

	tmplName := ""
	if actor.ActorTemplate != nil {
		tmplName = actor.ActorTemplate.Name
	}

	actor.Status = &ateapipb.ActorStatus{
		State: ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
	}

	s.actors[key] = actor
	s.actorStateData[key] = make(map[string]any)
	s.logs[key] = []string{fmt.Sprintf("[%s] Actor created in atespace %s from template %s", time.Now().Format(time.RFC3339), atespace, tmplName)}
	s.saveToDiskLocked()
	return actor, nil
}

func (s *Store) GetActor(atespace, name string) (*ateapipb.Actor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := ActorKey(atespace, name)
	actor, exists := s.actors[key]
	if !exists {
		return nil, fmt.Errorf("%w: actor %q in atespace %q", ErrNotFound, name, atespace)
	}
	return actor, nil
}

func (s *Store) ListActors(atespace string) ([]*ateapipb.Actor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]*ateapipb.Actor, 0)
	for _, act := range s.actors {
		if act.Metadata == nil {
			continue
		}
		if atespace == "" || act.Metadata.Atespace == atespace {
			res = append(res, act)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Metadata == nil || res[j].Metadata == nil {
			return false
		}
		if res[i].Metadata.Atespace != res[j].Metadata.Atespace {
			return res[i].Metadata.Atespace < res[j].Metadata.Atespace
		}
		return res[i].Metadata.Name < res[j].Metadata.Name
	})
	return res, nil
}

func (s *Store) UpdateActor(actor *ateapipb.Actor) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if actor == nil || actor.Metadata == nil {
		return nil, fmt.Errorf("%w: actor metadata is required", ErrInvalidArgument)
	}
	atespace := actor.Metadata.Atespace
	name := actor.Metadata.Name

	key := ActorKey(atespace, name)
	existing, exists := s.actors[key]
	if !exists {
		return nil, fmt.Errorf("%w: actor %q in atespace %q", ErrNotFound, name, atespace)
	}

	now := timestamppb.Now()
	existing.Metadata.UpdateTime = now
	existing.Metadata.Version++
	if actor.WorkerSelector != nil {
		existing.WorkerSelector = actor.WorkerSelector
	}

	s.saveToDiskLocked()
	return existing, nil
}

func (s *Store) SetActorState(atespace, name string, state ateapipb.ActorState, workerPod, workerIP string) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ActorKey(atespace, name)
	actor, exists := s.actors[key]
	if !exists {
		return nil, fmt.Errorf("%w: actor %q in atespace %q", ErrNotFound, name, atespace)
	}

	if actor.Status == nil {
		actor.Status = &ateapipb.ActorStatus{}
	}
	actor.Status.State = state

	if state == ateapipb.ActorState_ACTOR_STATE_RUNNING && workerPod != "" {
		actor.Status.WorkerAssignment = &ateapipb.WorkerAssignment{
			Worker: &ateapipb.ObjectRef{
				Name: workerPod,
			},
			WorkerNamespace: "default",
			WorkerPool:      "default",
			WorkerPod:       workerPod,
			WorkerPodUid:    uuid.NewString(),
			WorkerPodIp:     workerIP,
		}
	} else if state == ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		actor.Status.WorkerAssignment = nil
	}

	actor.Metadata.Version++
	actor.Metadata.UpdateTime = timestamppb.Now()

	s.saveToDiskLocked()
	return actor, nil
}

func (s *Store) DeleteActor(atespace, name string, anyState bool) (*ateapipb.Actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ActorKey(atespace, name)
	actor, exists := s.actors[key]
	if !exists {
		return nil, fmt.Errorf("%w: actor %q in atespace %q", ErrNotFound, name, atespace)
	}

	curState := ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	if actor.Status != nil {
		curState = actor.Status.State
	}

	if !anyState && curState != ateapipb.ActorState_ACTOR_STATE_SUSPENDED && curState != ateapipb.ActorState_ACTOR_STATE_CRASHED {
		return nil, fmt.Errorf("%w: cannot delete actor in state %s without any_state flag", ErrPrecondition, curState)
	}

	delete(s.actors, key)
	delete(s.actorStateData, key)
	delete(s.logs, key)
	delete(s.egressPolicies, key)
	s.saveToDiskLocked()
	return actor, nil
}

// --- Worker operations ---

func (s *Store) CreateWorker(worker *ateapipb.Worker) (*ateapipb.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker == nil || worker.Metadata == nil || worker.Metadata.Name == "" {
		return nil, fmt.Errorf("%w: worker metadata with name is required", ErrInvalidArgument)
	}
	name := worker.Metadata.Name
	if _, exists := s.workers[name]; exists {
		return nil, fmt.Errorf("%w: worker %q", ErrAlreadyExists, name)
	}

	now := timestamppb.Now()
	worker.Metadata.Uid = uuid.NewString()
	worker.Metadata.Version = 1
	worker.Metadata.CreateTime = now
	worker.Metadata.UpdateTime = now

	s.workers[name] = worker
	s.saveToDiskLocked()
	return worker, nil
}

func (s *Store) GetWorker(name string) (*ateapipb.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	w, exists := s.workers[name]
	if !exists {
		return nil, fmt.Errorf("%w: worker %q", ErrNotFound, name)
	}
	return w, nil
}

func (s *Store) ListWorkers() ([]*ateapipb.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]*ateapipb.Worker, 0, len(s.workers))
	for _, w := range s.workers {
		res = append(res, w)
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Metadata == nil || res[j].Metadata == nil {
			return false
		}
		var nA, nB int
		if n, _ := fmt.Sscanf(res[i].Metadata.Name, "worker-%d", &nA); n == 1 {
			if m, _ := fmt.Sscanf(res[j].Metadata.Name, "worker-%d", &nB); m == 1 {
				return nA < nB
			}
		}
		return res[i].Metadata.Name < res[j].Metadata.Name
	})
	return res, nil
}

func (s *Store) UpdateWorker(worker *ateapipb.Worker) (*ateapipb.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker == nil || worker.Metadata == nil {
		return nil, fmt.Errorf("%w: worker metadata is required", ErrInvalidArgument)
	}
	name := worker.Metadata.Name

	w, exists := s.workers[name]
	if !exists {
		return nil, fmt.Errorf("%w: worker %q", ErrNotFound, name)
	}

	now := timestamppb.Now()
	w.Status = worker.Status
	w.Labels = worker.Labels
	if w.Metadata != nil {
		w.Metadata.UpdateTime = now
		w.Metadata.Version++
	}

	s.saveToDiskLocked()
	return w, nil
}

func (s *Store) DeleteWorker(name string) (*ateapipb.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, exists := s.workers[name]
	if !exists {
		return nil, fmt.Errorf("%w: worker %q", ErrNotFound, name)
	}

	delete(s.workers, name)
	s.saveToDiskLocked()
	return w, nil
}

// --- Tag operations ---

func (s *Store) CreateTag(tag *ateapipb.Tag) (*ateapipb.Tag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tag == nil || tag.Metadata == nil {
		return nil, fmt.Errorf("%w: tag metadata is required", ErrInvalidArgument)
	}
	atespace := tag.Metadata.Atespace
	name := tag.Metadata.Name
	if atespace == "" || name == "" {
		return nil, fmt.Errorf("%w: atespace and name are required for tag", ErrInvalidArgument)
	}

	key := ActorKey(atespace, name)
	if _, exists := s.tags[key]; exists {
		return nil, fmt.Errorf("%w: tag %q in atespace %q", ErrAlreadyExists, name, atespace)
	}

	now := timestamppb.Now()
	tag.Metadata.Uid = uuid.NewString()
	tag.Metadata.Version = 1
	tag.Metadata.CreateTime = now
	tag.Metadata.UpdateTime = now

	s.tags[key] = tag
	s.saveToDiskLocked()
	return tag, nil
}

func (s *Store) GetTag(atespace, name string) (*ateapipb.Tag, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := ActorKey(atespace, name)
	tag, exists := s.tags[key]
	if !exists {
		return nil, fmt.Errorf("%w: tag %q in atespace %q", ErrNotFound, name, atespace)
	}
	return tag, nil
}

func (s *Store) ListTags(atespace string) ([]*ateapipb.Tag, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]*ateapipb.Tag, 0)
	for _, t := range s.tags {
		if t.Metadata == nil {
			continue
		}
		if atespace == "" || t.Metadata.Atespace == atespace {
			res = append(res, t)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		if res[i].Metadata == nil || res[j].Metadata == nil {
			return false
		}
		if res[i].Metadata.Atespace != res[j].Metadata.Atespace {
			return res[i].Metadata.Atespace < res[j].Metadata.Atespace
		}
		return res[i].Metadata.Name < res[j].Metadata.Name
	})
	return res, nil
}

func (s *Store) UpdateTag(tag *ateapipb.Tag) (*ateapipb.Tag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tag == nil || tag.Metadata == nil {
		return nil, fmt.Errorf("%w: tag metadata is required", ErrInvalidArgument)
	}
	key := ActorKey(tag.Metadata.Atespace, tag.Metadata.Name)
	t, exists := s.tags[key]
	if !exists {
		return nil, fmt.Errorf("%w: tag %q in atespace %q", ErrNotFound, tag.Metadata.Name, tag.Metadata.Atespace)
	}

	t.Scope = tag.Scope
	if t.Metadata != nil {
		t.Metadata.UpdateTime = timestamppb.Now()
		t.Metadata.Version++
	}
	s.saveToDiskLocked()
	return t, nil
}

func (s *Store) DeleteTag(atespace, name string) (*ateapipb.Tag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ActorKey(atespace, name)
	tag, exists := s.tags[key]
	if !exists {
		return nil, fmt.Errorf("%w: tag %q in atespace %q", ErrNotFound, name, atespace)
	}

	delete(s.tags, key)
	s.saveToDiskLocked()
	return tag, nil
}

// --- Egress Policy operations ---

func (s *Store) CreateActorEgressPolicy(ep *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ep == nil || ep.Metadata == nil {
		return nil, fmt.Errorf("%w: egress policy metadata required", ErrInvalidArgument)
	}
	atespace := ep.Metadata.Atespace
	actor := ep.Metadata.Name
	if atespace == "" || actor == "" {
		return nil, fmt.Errorf("%w: atespace and actor required", ErrInvalidArgument)
	}

	key := ActorKey(atespace, actor)
	if _, exists := s.egressPolicies[key]; exists {
		return nil, fmt.Errorf("%w: egress policy for actor %q", ErrAlreadyExists, actor)
	}

	now := timestamppb.Now()
	ep.Metadata.Uid = uuid.NewString()
	ep.Metadata.Version = 1
	ep.Metadata.CreateTime = now
	ep.Metadata.UpdateTime = now

	s.egressPolicies[key] = ep
	s.saveToDiskLocked()
	return ep, nil
}

func (s *Store) GetActorEgressPolicy(atespace, actor string) (*ateapipb.EgressPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := ActorKey(atespace, actor)
	ep, exists := s.egressPolicies[key]
	if !exists {
		return nil, fmt.Errorf("%w: egress policy for actor %q in atespace %q", ErrNotFound, actor, atespace)
	}
	return ep, nil
}

func (s *Store) UpdateActorEgressPolicy(ep *ateapipb.EgressPolicy) (*ateapipb.EgressPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ep == nil || ep.Metadata == nil {
		return nil, fmt.Errorf("%w: egress policy metadata required", ErrInvalidArgument)
	}
	atespace := ep.Metadata.Atespace
	actor := ep.Metadata.Name

	key := ActorKey(atespace, actor)
	existing, exists := s.egressPolicies[key]
	if !exists {
		return nil, fmt.Errorf("%w: egress policy for actor %q in atespace %q", ErrNotFound, actor, atespace)
	}

	existing.Rules = ep.Rules
	if existing.Metadata != nil {
		existing.Metadata.UpdateTime = timestamppb.Now()
		existing.Metadata.Version++
	}
	s.saveToDiskLocked()
	return existing, nil
}

func (s *Store) DeleteActorEgressPolicy(atespace, actor string) (*ateapipb.EgressPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ActorKey(atespace, actor)
	ep, exists := s.egressPolicies[key]
	if !exists {
		return nil, fmt.Errorf("%w: egress policy for actor %q in atespace %q", ErrNotFound, actor, atespace)
	}

	delete(s.egressPolicies, key)
	s.saveToDiskLocked()
	return ep, nil
}

// --- Logs & State Data operations ---

func (s *Store) AppendLog(atespace, actor, line string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ActorKey(atespace, actor)
	s.logs[key] = append(s.logs[key], line)
}

func (s *Store) GetLogs(atespace, actor string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := ActorKey(atespace, actor)
	lines := s.logs[key]
	res := make([]string, len(lines))
	copy(res, lines)
	return res
}

func (s *Store) GetActorStateData(atespace, actor string) map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := ActorKey(atespace, actor)
	data, ok := s.actorStateData[key]
	if !ok {
		return map[string]any{}
	}
	copied := make(map[string]any)
	for k, v := range data {
		copied[k] = v
	}
	return copied
}

func (s *Store) SetActorStateData(atespace, actor string, data map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := ActorKey(atespace, actor)
	s.actorStateData[key] = data
}

func (s *Store) SaveSnapshot(snap *SnapshotData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshots[snap.URI] = snap
}

func (s *Store) GetSnapshot(uri string) (*SnapshotData, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap, ok := s.snapshots[uri]
	return snap, ok
}

// --- Disk Persistence ---

type diskState struct {
	Atespaces      []json.RawMessage         `json:"atespaces"`
	Templates      []json.RawMessage         `json:"templates"`
	Actors         []json.RawMessage         `json:"actors"`
	Workers        []json.RawMessage         `json:"workers"`
	Tags           []json.RawMessage         `json:"tags"`
	EgressPolicies []json.RawMessage         `json:"egress_policies"`
	ActorStateData map[string]map[string]any `json:"actor_state_data"`
}

func (s *Store) saveToDiskLocked() {
	if !s.persist || s.stateDir == "" {
		return
	}

	ds := diskState{
		ActorStateData: s.actorStateData,
	}

	for _, as := range s.atespaces {
		if b, err := protojson.Marshal(as); err == nil {
			ds.Atespaces = append(ds.Atespaces, b)
		}
	}
	for _, tmpl := range s.templates {
		if b, err := protojson.Marshal(tmpl); err == nil {
			ds.Templates = append(ds.Templates, b)
		}
	}
	for _, act := range s.actors {
		if b, err := protojson.Marshal(act); err == nil {
			ds.Actors = append(ds.Actors, b)
		}
	}
	for _, w := range s.workers {
		if b, err := protojson.Marshal(w); err == nil {
			ds.Workers = append(ds.Workers, b)
		}
	}
	for _, t := range s.tags {
		if b, err := protojson.Marshal(t); err == nil {
			ds.Tags = append(ds.Tags, b)
		}
	}
	for _, ep := range s.egressPolicies {
		if b, err := protojson.Marshal(ep); err == nil {
			ds.EgressPolicies = append(ds.EgressPolicies, b)
		}
	}

	data, err := json.MarshalIndent(ds, "", "  ")
	if err != nil {
		return
	}

	stateFile := filepath.Join(s.stateDir, "state.json")
	_ = os.WriteFile(stateFile, data, 0644)
}

func (s *Store) loadFromDisk() error {
	stateFile := filepath.Join(s.stateDir, "state.json")
	data, err := os.ReadFile(stateFile)
	if err != nil {
		return err
	}

	var ds diskState
	if err := json.Unmarshal(data, &ds); err != nil {
		return err
	}

	for _, raw := range ds.Atespaces {
		var as ateapipb.Atespace
		if err := protojson.Unmarshal(raw, &as); err == nil && as.Metadata != nil {
			s.atespaces[as.Metadata.Name] = &as
		}
	}
	for _, raw := range ds.Templates {
		var tmpl ateapipb.ActorTemplate
		if err := protojson.Unmarshal(raw, &tmpl); err == nil && tmpl.Metadata != nil {
			s.templates[ActorKey(tmpl.Metadata.Atespace, tmpl.Metadata.Name)] = &tmpl
		}
	}
	for _, raw := range ds.Actors {
		var act ateapipb.Actor
		if err := protojson.Unmarshal(raw, &act); err == nil && act.Metadata != nil {
			if act.Status != nil {
				act.Status.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
				act.Status.WorkerAssignment = nil
			}
			s.actors[ActorKey(act.Metadata.Atespace, act.Metadata.Name)] = &act
		}
	}
	for _, raw := range ds.Workers {
		var w ateapipb.Worker
		if err := protojson.Unmarshal(raw, &w); err == nil && w.Metadata != nil {
			if w.Status != nil {
				w.Status.State = ateapipb.WorkerState_WORKER_STATE_ACTIVE
				if w.Status.Allocated != nil {
					w.Status.Allocated.Actors = 0
				}
			}
			s.workers[w.Metadata.Name] = &w
		}
	}
	for _, raw := range ds.Tags {
		var t ateapipb.Tag
		if err := protojson.Unmarshal(raw, &t); err == nil && t.Metadata != nil {
			s.tags[ActorKey(t.Metadata.Atespace, t.Metadata.Name)] = &t
		}
	}
	for _, raw := range ds.EgressPolicies {
		var ep ateapipb.EgressPolicy
		if err := protojson.Unmarshal(raw, &ep); err == nil && ep.Metadata != nil {
			s.egressPolicies[ActorKey(ep.Metadata.Atespace, ep.Metadata.Name)] = &ep
		}
	}
	if ds.ActorStateData != nil {
		s.actorStateData = ds.ActorStateData
	}

	return nil
}
