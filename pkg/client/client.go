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

package client

import (
	"context"
	"fmt"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	Control ateapipb.ControlClient
	Worker  ateapipb.WorkerServiceClient
	conn    *grpc.ClientConn
}

func NewClient(endpoint string) (*Client, error) {
	if endpoint == "" {
		endpoint = "127.0.0.1:8080"
	}

	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to miniate server at %s: %w", endpoint, err)
	}

	return &Client{
		Control: ateapipb.NewControlClient(conn),
		Worker:  ateapipb.NewWorkerServiceClient(conn),
		conn:    conn,
	}, nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.Control.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{})
	return err
}
