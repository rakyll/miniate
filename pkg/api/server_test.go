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

package api_test

import (
	"context"
	"net"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/rakyll/miniate/pkg/api"
	"github.com/rakyll/miniate/pkg/runtime"
	"github.com/rakyll/miniate/pkg/store"
)

func TestGRPCServer(t *testing.T) {
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

	srv := api.NewServer(st, eng)
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial gRPC: %v", err)
	}
	defer conn.Close()

	client := ateapipb.NewControlClient(conn)
	ctx := context.Background()

	// 1. Create Atespace
	as, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{Metadata: &ateapipb.ResourceMetadata{Name: "demo"}},
	})
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if as.Metadata.Name != "demo" {
		t.Errorf("expected demo, got %s", as.Metadata.Name)
	}

	// 2. Create Template
	tmpl, err := client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "demo",
				Name:     "counter",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}
	if tmpl.Metadata.Name != "counter" {
		t.Errorf("expected counter, got %s", tmpl.Metadata.Name)
	}

	// 3. Create Actor
	act, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "demo",
				Name:     "counter-1",
			},
			ActorTemplate: &ateapipb.ObjectRef{
				Atespace: "demo",
				Name:     "counter",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if act.Status.State != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("expected SUSPENDED, got %s", act.Status.State)
	}

	// 4. Resume Actor
	resResp, err := client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: "demo",
			Name:     "counter-1",
		},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	if resResp.Actor.Status.State != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("expected RUNNING, got %s", resResp.Actor.Status.State)
	}

	// 5. Mint Actor JWT
	jwtResp, err := client.MintActorJWT(ctx, &ateapipb.MintActorJWTRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: "demo",
			Name:     "counter-1",
		},
		Audience: []string{"https://api.example.com"},
	})
	if err != nil {
		t.Fatalf("MintActorJWT failed: %v", err)
	}
	if jwtResp.ActorJwt == "" {
		t.Errorf("expected non-empty JWT")
	}

	// 6. Suspend Actor
	susResp, err := client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{
			Atespace: "demo",
			Name:     "counter-1",
		},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if susResp.Actor.Status.State != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("expected SUSPENDED, got %s", susResp.Actor.Status.State)
	}
}
