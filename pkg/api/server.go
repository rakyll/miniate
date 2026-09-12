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

package api

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/rakyll/miniate/pkg/runtime"
	"github.com/rakyll/miniate/pkg/store"
)

type Server struct {
	ateapipb.UnimplementedControlServer
	ateapipb.UnimplementedWorkerServiceServer

	st  *store.Store
	eng *runtime.Engine
}

func NewServer(st *store.Store, eng *runtime.Engine) *Server {
	return &Server{
		st:  st,
		eng: eng,
	}
}

func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, store.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	if errors.Is(err, store.ErrAlreadyExists) {
		return status.Error(codes.AlreadyExists, err.Error())
	}
	if errors.Is(err, store.ErrPrecondition) {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if errors.Is(err, store.ErrInvalidArgument) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, runtime.ErrNoWorkersAvailable) {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

// --- Atespaces ---

func (s *Server) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	if req.Atespace == nil || req.Atespace.Metadata == nil || req.Atespace.Metadata.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace metadata name is required")
	}
	as, err := s.st.CreateAtespace(req.Atespace.Metadata.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return as, nil
}

func (s *Server) GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.Atespace, error) {
	if req.Atespace == nil || req.Atespace.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace name is required")
	}
	as, err := s.st.GetAtespace(req.Atespace.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return as, nil
}

func (s *Server) ListAtespaces(ctx context.Context, req *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	list, err := s.st.ListAtespaces()
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &ateapipb.ListAtespacesResponse{
		Atespaces: list,
	}, nil
}

func (s *Server) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.Atespace, error) {
	if req.Atespace == nil || req.Atespace.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace name is required")
	}
	as, err := s.st.DeleteAtespace(req.Atespace.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return as, nil
}

// --- Actor Templates ---

func (s *Server) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if req.ActorTemplate == nil {
		return nil, status.Error(codes.InvalidArgument, "actor_template is required")
	}
	tmpl, err := s.st.CreateActorTemplate(req.ActorTemplate)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return tmpl, nil
}

func (s *Server) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if req.ActorTemplate == nil || req.ActorTemplate.Atespace == "" || req.ActorTemplate.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace and name are required")
	}
	tmpl, err := s.st.GetActorTemplate(req.ActorTemplate.Atespace, req.ActorTemplate.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return tmpl, nil
}

func (s *Server) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	list, err := s.st.ListActorTemplates(req.Atespace)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &ateapipb.ListActorTemplatesResponse{
		ActorTemplates: list,
	}, nil
}

func (s *Server) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	if req.ActorTemplate == nil || req.ActorTemplate.Atespace == "" || req.ActorTemplate.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "atespace and name are required")
	}
	tmpl, err := s.st.DeleteActorTemplate(req.ActorTemplate.Atespace, req.ActorTemplate.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return tmpl, nil
}

// --- Actors ---

func (s *Server) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	if req.Actor == nil {
		return nil, status.Error(codes.InvalidArgument, "actor is required")
	}
	actor, err := s.st.CreateActor(req.Actor)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return actor, nil
}

func (s *Server) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}
	actor, err := s.st.GetActor(req.Actor.Atespace, req.Actor.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return actor, nil
}

func (s *Server) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	list, err := s.st.ListActors(req.Atespace)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &ateapipb.ListActorsResponse{
		Actors: list,
	}, nil
}

func (s *Server) UpdateActor(ctx context.Context, req *ateapipb.UpdateActorRequest) (*ateapipb.Actor, error) {
	if req.Actor == nil {
		return nil, status.Error(codes.InvalidArgument, "actor is required")
	}
	actor, err := s.st.UpdateActor(req.Actor)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return actor, nil
}

func (s *Server) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}
	actor, err := s.eng.ResumeActor(ctx, req.Actor.Atespace, req.Actor.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &ateapipb.ResumeActorResponse{
		Actor:   actor,
		Resumed: true,
	}, nil
}

func (s *Server) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}
	actor, _, err := s.eng.SuspendActor(ctx, req.Actor.Atespace, req.Actor.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &ateapipb.SuspendActorResponse{
		Actor: actor,
	}, nil
}

func (s *Server) PauseActor(ctx context.Context, req *ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}
	actor, err := s.eng.PauseActor(ctx, req.Actor.Atespace, req.Actor.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &ateapipb.PauseActorResponse{
		Actor: actor,
	}, nil
}

func (s *Server) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}

	if req.AnyState {
		_, _, _ = s.eng.SuspendActor(ctx, req.Actor.Atespace, req.Actor.Name)
	}

	actor, err := s.st.DeleteActor(req.Actor.Atespace, req.Actor.Name, req.AnyState)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return actor, nil
}

// --- Workers ---

func (s *Server) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	workers, err := s.st.ListWorkers()
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &ateapipb.ListWorkersResponse{
		Workers: workers,
	}, nil
}

func (s *Server) GetWorker(ctx context.Context, req *ateapipb.GetWorkerRequest) (*ateapipb.Worker, error) {
	if req.Worker == nil || req.Worker.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "worker name is required")
	}
	worker, err := s.st.GetWorker(req.Worker.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return worker, nil
}

func (s *Server) CreateWorker(ctx context.Context, req *ateapipb.CreateWorkerRequest) (*ateapipb.Worker, error) {
	if req.Worker == nil {
		return nil, status.Error(codes.InvalidArgument, "worker is required")
	}
	w, err := s.st.CreateWorker(req.Worker)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return w, nil
}

func (s *Server) UpdateWorker(ctx context.Context, req *ateapipb.UpdateWorkerRequest) (*ateapipb.Worker, error) {
	if req.Worker == nil {
		return nil, status.Error(codes.InvalidArgument, "worker is required")
	}
	w, err := s.st.UpdateWorker(req.Worker)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return w, nil
}

func (s *Server) DeleteWorker(ctx context.Context, req *ateapipb.DeleteWorkerRequest) (*ateapipb.Worker, error) {
	if req.Worker == nil || req.Worker.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "worker name is required")
	}
	w, err := s.st.DeleteWorker(req.Worker.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return w, nil
}

func (s *Server) DrainWorker(ctx context.Context, req *ateapipb.DrainWorkerRequest) (*ateapipb.Worker, error) {
	if req.Worker == nil || req.Worker.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "worker name is required")
	}
	w, err := s.st.GetWorker(req.Worker.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	if w.Status == nil {
		w.Status = &ateapipb.WorkerStatus{}
	}
	w.Status.State = ateapipb.WorkerState_WORKER_STATE_DRAINING
	if _, err := s.st.UpdateWorker(w); err != nil {
		return nil, toGRPCError(err)
	}
	return w, nil
}

func (s *Server) ListWorkerActorAssignments(ctx context.Context, req *ateapipb.ListWorkerActorAssignmentsRequest) (*ateapipb.ListWorkerActorAssignmentsResponse, error) {
	actors, err := s.st.ListActors("")
	if err != nil {
		return nil, toGRPCError(err)
	}
	var assignments []*ateapipb.ActorAssignment
	for _, a := range actors {
		if a.Status != nil && a.Status.WorkerAssignment != nil {
			targetWorker := ""
			if req.Worker != nil {
				targetWorker = req.Worker.Name
			}
			if targetWorker == "" || a.Status.WorkerAssignment.WorkerPod == targetWorker {
				assignments = append(assignments, &ateapipb.ActorAssignment{
					Actor: &ateapipb.ObjectRef{
						Atespace: a.Metadata.Atespace,
						Name:     a.Metadata.Name,
					},
					ActorUid: a.Metadata.Uid,
					ActorTemplateRef: a.ActorTemplate,
				})
			}
		}
	}
	return &ateapipb.ListWorkerActorAssignmentsResponse{
		ActorAssignments: assignments,
	}, nil
}

// --- WorkerService ---

func (s *Server) SetWorkerCapacity(ctx context.Context, req *ateapipb.SetWorkerCapacityRequest) (*ateapipb.SetWorkerCapacityResponse, error) {
	return &ateapipb.SetWorkerCapacityResponse{}, nil
}

// --- Tags ---

func (s *Server) CreateTag(ctx context.Context, req *ateapipb.CreateTagRequest) (*ateapipb.Tag, error) {
	if req.Tag == nil {
		return nil, status.Error(codes.InvalidArgument, "tag is required")
	}
	tag, err := s.st.CreateTag(req.Tag)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return tag, nil
}

func (s *Server) GetTag(ctx context.Context, req *ateapipb.GetTagRequest) (*ateapipb.Tag, error) {
	if req.Tag == nil || req.Tag.Atespace == "" || req.Tag.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "tag atespace and name are required")
	}
	tag, err := s.st.GetTag(req.Tag.Atespace, req.Tag.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return tag, nil
}

func (s *Server) ListTags(ctx context.Context, req *ateapipb.ListTagsRequest) (*ateapipb.ListTagsResponse, error) {
	tags, err := s.st.ListTags(req.Atespace)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return &ateapipb.ListTagsResponse{
		Tags: tags,
	}, nil
}

func (s *Server) UpdateTag(ctx context.Context, req *ateapipb.UpdateTagRequest) (*ateapipb.Tag, error) {
	if req.Tag == nil {
		return nil, status.Error(codes.InvalidArgument, "tag is required")
	}
	tag, err := s.st.UpdateTag(req.Tag)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return tag, nil
}

func (s *Server) DeleteTag(ctx context.Context, req *ateapipb.DeleteTagRequest) (*ateapipb.Tag, error) {
	if req.Tag == nil || req.Tag.Atespace == "" || req.Tag.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "tag atespace and name are required")
	}
	tag, err := s.st.DeleteTag(req.Tag.Atespace, req.Tag.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return tag, nil
}

// --- Egress Policies ---

func (s *Server) CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if req.EgressPolicy == nil {
		return nil, status.Error(codes.InvalidArgument, "egress_policy is required")
	}
	ep, err := s.st.CreateActorEgressPolicy(req.EgressPolicy)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return ep, nil
}

func (s *Server) GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}
	ep, err := s.st.GetActorEgressPolicy(req.Actor.Atespace, req.Actor.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return ep, nil
}

func (s *Server) UpdateActorEgressPolicy(ctx context.Context, req *ateapipb.UpdateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if req.EgressPolicy == nil {
		return nil, status.Error(codes.InvalidArgument, "egress_policy is required")
	}
	ep, err := s.st.UpdateActorEgressPolicy(req.EgressPolicy)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return ep, nil
}

func (s *Server) DeleteActorEgressPolicy(ctx context.Context, req *ateapipb.DeleteActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}
	ep, err := s.st.DeleteActorEgressPolicy(req.Actor.Atespace, req.Actor.Name)
	if err != nil {
		return nil, toGRPCError(err)
	}
	return ep, nil
}

// --- Identity / JWT / Certificates ---

func (s *Server) MintActorJWT(ctx context.Context, req *ateapipb.MintActorJWTRequest) (*ateapipb.MintActorJWTResponse, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := map[string]any{
		"iss":      "miniate-control-plane",
		"sub":      fmt.Sprintf("actor:%s:%s", req.Actor.Atespace, req.Actor.Name),
		"aud":      req.Audience,
		"atespace": req.Actor.Atespace,
		"actor":    req.Actor.Name,
		"iat":      time.Now().Unix(),
		"exp":      time.Now().Add(1 * time.Hour).Unix(),
	}
	claimsBytes, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(claimsBytes)
	sig := base64.RawURLEncoding.EncodeToString([]byte("miniate-dev-signature"))

	token := fmt.Sprintf("%s.%s.%s", header, payload, sig)
	return &ateapipb.MintActorJWTResponse{
		ActorJwt: token,
	}, nil
}

func (s *Server) MintActorCertificate(ctx context.Context, req *ateapipb.MintActorCertificateRequest) (*ateapipb.MintActorCertificateResponse, error) {
	if req.Actor == nil || req.Actor.Atespace == "" || req.Actor.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "actor atespace and name are required")
	}

	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName:   fmt.Sprintf("%s.%s.miniate.local", req.Actor.Name, req.Actor.Atespace),
			Organization: []string{"Agent Substrate Miniate"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, _ := x509.CreateCertificate(rand.Reader, &template, &template, &privKey.PublicKey, privKey)

	return &ateapipb.MintActorCertificateResponse{
		ActorCertificates: [][]byte{certDER},
	}, nil
}
