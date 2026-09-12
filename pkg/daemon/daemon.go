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

package daemon

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"github.com/rakyll/miniate/pkg/api"
	"github.com/rakyll/miniate/pkg/dashboard"
	"github.com/rakyll/miniate/pkg/router"
	"github.com/rakyll/miniate/pkg/runtime"
	"github.com/rakyll/miniate/pkg/store"
)

type Config struct {
	StateDir      string
	GRPCPort      int
	RouterPort    int
	DashboardPort int
	WorkerCount   int
}

func DefaultConfig() Config {
	home, _ := os.UserHomeDir()
	return Config{
		StateDir:      filepath.Join(home, ".miniate"),
		GRPCPort:      8080,
		RouterPort:    8000,
		DashboardPort: 8082,
		WorkerCount:   8,
	}
}

func PIDFilePath(stateDir string) string {
	return filepath.Join(stateDir, "miniate.pid")
}

func IsRunning(stateDir string) (bool, int) {
	pidPath := PIDFilePath(stateDir)
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return false, 0
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return false, 0
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		return false, 0
	}

	// Sending signal 0 checks if process exists without killing it
	if err := process.Signal(syscall.Signal(0)); err == nil {
		return true, pid
	}

	_ = os.Remove(pidPath)
	return false, 0
}

func StartDaemon(cfg Config) error {
	if running, pid := IsRunning(cfg.StateDir); running {
		return fmt.Errorf("miniate is already running (PID: %d)", pid)
	}

	if err := os.MkdirAll(cfg.StateDir, 0755); err != nil {
		return fmt.Errorf("failed to create state dir: %w", err)
	}

	executable, err := os.Executable()
	if err != nil {
		return err
	}

	logFile, err := os.OpenFile(filepath.Join(cfg.StateDir, "miniate.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	cmd := exec.Command(executable, "start", "--foreground",
		fmt.Sprintf("--grpc-port=%d", cfg.GRPCPort),
		fmt.Sprintf("--router-port=%d", cfg.RouterPort),
		fmt.Sprintf("--dashboard-port=%d", cfg.DashboardPort),
		fmt.Sprintf("--workers=%d", cfg.WorkerCount),
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start background process: %w", err)
	}

	pidPath := PIDFilePath(cfg.StateDir)
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	// Wait briefly to verify start
	time.Sleep(500 * time.Millisecond)
	if running, _ := IsRunning(cfg.StateDir); !running {
		return fmt.Errorf("daemon failed to start; check %s/miniate.log for details", cfg.StateDir)
	}

	return nil
}

func StopDaemon(stateDir string) error {
	running, pid := IsRunning(stateDir)
	if !running {
		return fmt.Errorf("miniate is not running")
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}

	_ = proc.Signal(syscall.SIGTERM)

	// Wait for process to exit
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		if running, _ := IsRunning(stateDir); !running {
			_ = os.Remove(PIDFilePath(stateDir))
			return nil
		}
	}

	// Force kill if not exited
	_ = proc.Signal(syscall.SIGKILL)
	_ = os.Remove(PIDFilePath(stateDir))
	return nil
}

func RunServer(ctx context.Context, cfg Config) error {
	if err := os.MkdirAll(cfg.StateDir, 0755); err != nil {
		return err
	}

	st, err := store.NewStore(cfg.StateDir, true)
	if err != nil {
		return fmt.Errorf("failed to initialize store: %w", err)
	}

	eng, err := runtime.NewEngine(st, runtime.EngineConfig{
		WorkerCount:    cfg.WorkerCount,
		BaseWorkerPort: 9100,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize runtime engine: %w", err)
	}

	srv := api.NewServer(st, eng)
	grpcServer := grpc.NewServer()
	ateapipb.RegisterControlServer(grpcServer, srv)
	ateapipb.RegisterWorkerServiceServer(grpcServer, srv)
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port %d: %w", cfg.GRPCPort, err)
	}

	// Start gRPC server in background
	go func() {
		if err := grpcServer.Serve(grpcListener); err != nil {
			fmt.Printf("gRPC server terminated: %v\n", err)
		}
	}()

	// Start HTTP Traffic Router on router port
	r := router.NewRouter(st, eng, fmt.Sprintf(":%d", cfg.RouterPort))
	go func() {
		if err := r.Start(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP router terminated: %v\n", err)
		}
	}()

	// Start Dashboard Web UI
	dash := dashboard.NewDashboard(st, eng)
	dashMux := http.NewServeMux()
	dash.RegisterRoutes(dashMux)
	dashServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.DashboardPort),
		Handler: dashMux,
	}
	go func() {
		if err := dashServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Dashboard server terminated: %v\n", err)
		}
	}()

	// Record PID
	_ = os.WriteFile(PIDFilePath(cfg.StateDir), []byte(strconv.Itoa(os.Getpid())), 0644)

	fmt.Printf("✨ Miniate local Substrate control plane is ready!\n")
	fmt.Printf("   ├─ gRPC API Endpoint:  localhost:%d\n", cfg.GRPCPort)
	fmt.Printf("   ├─ Traffic Router:     http://localhost:%d\n", cfg.RouterPort)
	fmt.Printf("   ├─ Web Dashboard:      http://localhost:%d/dashboard\n", cfg.DashboardPort)
	fmt.Printf("   └─ Physical Workers:   %d active slots\n", cfg.WorkerCount)

	// Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
	case <-sigCh:
	}

	fmt.Println("\nShutting down miniate gracefully...")
	grpcServer.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = r.Shutdown(shutdownCtx)
	_ = dashServer.Shutdown(shutdownCtx)
	_ = os.Remove(PIDFilePath(cfg.StateDir))

	return nil
}
