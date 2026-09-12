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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"
	"github.com/rakyll/miniate/pkg/client"
	"github.com/rakyll/miniate/pkg/daemon"
	"sigs.k8s.io/yaml"
)

const versionString = "v0.1.0-alpha"

var (
	grpcEndpoint string
	outputFmt    string
	cfg          daemon.Config
	foreground   bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "miniate",
		Short: "Local developer runtime for Agent Substrate",
		Long: `miniate is a lightweight local development environment for Agent Substrate,
providing a drop-in local control plane, simulated worker pool, traffic router,
and embedded developer dashboard.`,
	}

	cfg = daemon.DefaultConfig()

	rootCmd.PersistentFlags().StringVar(&grpcEndpoint, "endpoint", "127.0.0.1:8080", "Address of the miniate gRPC control plane")
	rootCmd.PersistentFlags().StringVarP(&outputFmt, "output", "o", "table", "Output format: table|json|yaml")

	// --- start ---
	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the local miniate control plane and worker runtime",
		RunE: func(cmd *cobra.Command, args []string) error {
			if foreground {
				return daemon.RunServer(context.Background(), cfg)
			}
			if err := daemon.StartDaemon(cfg); err != nil {
				return err
			}
			fmt.Printf("🚀 Miniate started successfully in background!\n")
			fmt.Printf("   Control plane: localhost:%d\n", cfg.GRPCPort)
			fmt.Printf("   Traffic Router: http://localhost:%d\n", cfg.RouterPort)
			fmt.Printf("   Dashboard:     http://localhost:%d/dashboard\n", cfg.DashboardPort)
			fmt.Printf("   Workers:       %d slots\n", cfg.WorkerCount)
			fmt.Println("\nRun 'miniate status' to check health, or 'miniate stop' to stop.")
			return nil
		},
	}
	startCmd.Flags().BoolVar(&foreground, "foreground", false, "Run miniate in the foreground")
	startCmd.Flags().IntVar(&cfg.GRPCPort, "grpc-port", 8080, "gRPC server port")
	startCmd.Flags().IntVar(&cfg.RouterPort, "router-port", 8000, "Traffic router port")
	startCmd.Flags().IntVar(&cfg.DashboardPort, "dashboard-port", 8082, "Dashboard web port")
	startCmd.Flags().IntVar(&cfg.WorkerCount, "workers", 8, "Number of worker slots")
	rootCmd.AddCommand(startCmd)

	// --- stop ---
	stopCmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the background miniate daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := daemon.StopDaemon(cfg.StateDir); err != nil {
				return err
			}
			fmt.Println("🛑 Miniate daemon stopped.")
			return nil
		},
	}
	rootCmd.AddCommand(stopCmd)

	// --- status ---
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Check the status of the local miniate cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			running, pid := daemon.IsRunning(cfg.StateDir)
			if !running {
				fmt.Println("Miniate is NOT running.")
				fmt.Println("Run 'miniate start' to start the local cluster.")
				return nil
			}

			fmt.Printf("Miniate Status: RUNNING (PID: %d)\n", pid)
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				fmt.Printf("Warning: Unable to reach gRPC control plane at %s: %v\n", grpcEndpoint, err)
				return nil
			}
			defer c.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			workers, _ := c.Control.ListWorkers(ctx, &ateapipb.ListWorkersRequest{})
			actors, _ := c.Control.ListActors(ctx, &ateapipb.ListActorsRequest{})
			atespaces, _ := c.Control.ListAtespaces(ctx, &ateapipb.ListAtespacesRequest{})

			totalW := 0
			assignedW := 0
			if workers != nil {
				totalW = len(workers.Workers)
				for _, w := range workers.Workers {
					if w.Status != nil && w.Status.Allocated != nil && w.Status.Allocated.Actors > 0 {
						assignedW++
					}
				}
			}

			totalA := 0
			runningA := 0
			if actors != nil {
				totalA = len(actors.Actors)
				for _, a := range actors.Actors {
					if a.Status != nil && a.Status.State == ateapipb.ActorState_ACTOR_STATE_RUNNING {
						runningA++
					}
				}
			}

			totalAS := 0
			if atespaces != nil {
				totalAS = len(atespaces.Atespaces)
			}

			fmt.Printf("  • Workers:   %d/%d assigned (%d free)\n", assignedW, totalW, totalW-assignedW)
			fmt.Printf("  • Actors:    %d total (%d running, %d suspended)\n", totalA, runningA, totalA-runningA)
			fmt.Printf("  • Atespaces: %d active\n", totalAS)
			fmt.Printf("  • Dashboard: http://localhost:8082/dashboard\n")
			fmt.Printf("  • Router:    http://localhost:8000/\n")
			return nil
		},
	}
	rootCmd.AddCommand(statusCmd)

	// --- delete ---
	deleteClusterCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete local cluster state and stop daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			_ = daemon.StopDaemon(cfg.StateDir)
			if err := os.RemoveAll(cfg.StateDir); err != nil {
				return fmt.Errorf("failed to clean state directory: %w", err)
			}
			fmt.Println("🗑️  Miniate local state deleted.")
			return nil
		},
	}
	rootCmd.AddCommand(deleteClusterCmd)

	// --- dashboard ---
	dashboardCmd := &cobra.Command{
		Use:   "dashboard",
		Short: "Display dashboard URL",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("🌐 Miniate Dashboard available at: http://localhost:8082/dashboard")
		},
	}
	rootCmd.AddCommand(dashboardCmd)

	// --- atespace subcommands ---
	atespaceCmd := &cobra.Command{
		Use:     "atespace",
		Aliases: []string{"atespaces"},
		Short:   "Manage atespaces (isolation boundaries)",
	}

	var atespaceCreateCmd = &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new atespace",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			as, err := c.Control.CreateAtespace(context.Background(), &ateapipb.CreateAtespaceRequest{
				Atespace: &ateapipb.Atespace{
					Metadata: &ateapipb.ResourceMetadata{Name: args[0]},
				},
			})
			if err != nil {
				return err
			}
			fmt.Printf("atespace %q created\n", as.Metadata.Name)
			return nil
		},
	}

	var atespaceListCmd = &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "get"},
		Short:   "List atespaces",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			resp, err := c.Control.ListAtespaces(context.Background(), &ateapipb.ListAtespacesRequest{})
			if err != nil {
				return err
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintln(w, "NAME\tCREATED AT")
			for _, as := range resp.Atespaces {
				name := ""
				created := "unknown"
				if as.Metadata != nil {
					name = as.Metadata.Name
					if as.Metadata.CreateTime != nil {
						created = as.Metadata.CreateTime.AsTime().Format(time.RFC3339)
					}
				}
				fmt.Fprintf(w, "%s\t%s\n", name, created)
			}
			w.Flush()
			return nil
		},
	}

	var atespaceDeleteCmd = &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an empty atespace",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			_, err = c.Control.DeleteAtespace(context.Background(), &ateapipb.DeleteAtespaceRequest{
				Atespace: &ateapipb.ObjectRef{Name: args[0]},
			})
			if err != nil {
				return err
			}
			fmt.Printf("atespace %q deleted\n", args[0])
			return nil
		},
	}

	atespaceCmd.AddCommand(atespaceCreateCmd, atespaceListCmd, atespaceDeleteCmd)
	rootCmd.AddCommand(atespaceCmd)

	// --- template subcommands ---
	var tmplAtespace string
	var tmplFile string

	templateCmd := &cobra.Command{
		Use:     "template",
		Aliases: []string{"templates", "actor-template", "actor-templates"},
		Short:   "Manage actor templates",
	}
	templateCmd.PersistentFlags().StringVarP(&tmplAtespace, "atespace", "a", "default", "Atespace for template")

	var tmplCreateCmd = &cobra.Command{
		Use:   "create",
		Short: "Create an actor template (-f template.yaml or JSON)",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			var data []byte
			if tmplFile == "-" {
				data, err = io.ReadAll(os.Stdin)
			} else if tmplFile != "" {
				data, err = os.ReadFile(tmplFile)
			} else if len(args) > 0 {
				tmpl := &ateapipb.ActorTemplate{
					Metadata: &ateapipb.ResourceMetadata{
						Atespace: tmplAtespace,
						Name:     args[0],
					},
					Containers: []*ateapipb.Container{
						{Name: args[0], Image: "ghcr.io/agent-substrate/counter:latest"},
					},
					SnapshotsConfig: &ateapipb.SnapshotsConfig{},
					SandboxConfig: &ateapipb.SandboxConfig{
						SandboxClass: ateapipb.SandboxClass_SANDBOX_CLASS_GVISOR,
					},
				}
				res, err := c.Control.CreateActorTemplate(context.Background(), &ateapipb.CreateActorTemplateRequest{
					ActorTemplate: tmpl,
				})
				if err != nil {
					return err
				}
				fmt.Printf("actor template %s/%s created\n", res.Metadata.Atespace, res.Metadata.Name)
				return nil
			} else {
				return fmt.Errorf("must specify -f <manifest> or template name")
			}
			if err != nil {
				return err
			}

			jsonData, err := yaml.YAMLToJSON(data)
			if err != nil {
				jsonData = data
			}

			var tmpl ateapipb.ActorTemplate
			if err := protojson.Unmarshal(jsonData, &tmpl); err != nil {
				return fmt.Errorf("invalid template YAML/JSON: %w", err)
			}

			if tmpl.Metadata == nil {
				tmpl.Metadata = &ateapipb.ResourceMetadata{Atespace: tmplAtespace}
			}
			if tmpl.Metadata.Atespace == "" {
				tmpl.Metadata.Atespace = tmplAtespace
			}

			res, err := c.Control.CreateActorTemplate(context.Background(), &ateapipb.CreateActorTemplateRequest{
				ActorTemplate: &tmpl,
			})
			if err != nil {
				return err
			}
			fmt.Printf("actor template %s/%s created\n", res.Metadata.Atespace, res.Metadata.Name)
			return nil
		},
	}
	tmplCreateCmd.Flags().StringVarP(&tmplFile, "file", "f", "", "Path to template file (or - for stdin)")

	var tmplListCmd = &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "get"},
		Short:   "List actor templates",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			resp, err := c.Control.ListActorTemplates(context.Background(), &ateapipb.ListActorTemplatesRequest{
				Atespace: tmplAtespace,
			})
			if err != nil {
				return err
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintln(w, "ATESPACE\tNAME\tSANDBOX CLASS\tGOLDEN SNAPSHOT")
			for _, t := range resp.ActorTemplates {
				as := "default"
				name := ""
				if t.Metadata != nil {
					as = t.Metadata.Atespace
					name = t.Metadata.Name
				}
				sc := "GVISOR"
				if t.SandboxConfig != nil {
					sc = t.SandboxConfig.SandboxClass.String()
				}
				golden := ""
				if t.Status != nil && t.Status.GoldenSnapshotStatus != nil && t.Status.GoldenSnapshotStatus.GoldenSnapshot != nil {
					golden = t.Status.GoldenSnapshotStatus.GoldenSnapshot.SnapshotUri
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", as, name, sc, golden)
			}
			w.Flush()
			return nil
		},
	}

	var tmplDeleteCmd = &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an actor template",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			_, err = c.Control.DeleteActorTemplate(context.Background(), &ateapipb.DeleteActorTemplateRequest{
				ActorTemplate: &ateapipb.ObjectRef{
					Atespace: tmplAtespace,
					Name:     args[0],
				},
			})
			if err != nil {
				return err
			}
			fmt.Printf("actor template %s/%s deleted\n", tmplAtespace, args[0])
			return nil
		},
	}

	templateCmd.AddCommand(tmplCreateCmd, tmplListCmd, tmplDeleteCmd)
	rootCmd.AddCommand(templateCmd)

	// --- actor subcommands ---
	var actorAtespace string
	var actorTemplateName string
	var allAtespaces bool
	var anyState bool

	actorCmd := &cobra.Command{
		Use:     "actor",
		Aliases: []string{"actors"},
		Short:   "Manage actors and their lifecycles",
	}
	actorCmd.PersistentFlags().StringVarP(&actorAtespace, "atespace", "a", "default", "Atespace for actor")
	actorCmd.PersistentFlags().BoolVarP(&allAtespaces, "all-atespaces", "A", false, "Span all atespaces")

	var actorCreateCmd = &cobra.Command{
		Use:   "create <name>",
		Short: "Create an actor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			act := &ateapipb.Actor{
				Metadata: &ateapipb.ResourceMetadata{
					Atespace: actorAtespace,
					Name:     args[0],
				},
				ActorTemplate: &ateapipb.ObjectRef{
					Atespace: actorAtespace,
					Name:     actorTemplateName,
				},
			}
			res, err := c.Control.CreateActor(context.Background(), &ateapipb.CreateActorRequest{Actor: act})
			if err != nil {
				return err
			}
			stateStr := "SUSPENDED"
			if res.Status != nil {
				stateStr = res.Status.State.String()
			}
			fmt.Printf("actor %s/%s created (state: %s)\n", res.Metadata.Atespace, res.Metadata.Name, stateStr)
			return nil
		},
	}
	actorCreateCmd.Flags().StringVar(&actorTemplateName, "template", "counter", "Actor template name")

	var actorListCmd = &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "get"},
		Short:   "List actors",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			asQuery := actorAtespace
			if allAtespaces {
				asQuery = ""
			}

			resp, err := c.Control.ListActors(context.Background(), &ateapipb.ListActorsRequest{Atespace: asQuery})
			if err != nil {
				return err
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintln(w, "ATESPACE\tNAME\tTEMPLATE\tSTATE\tWORKER POD\tWORKER IP\tVERSION")
			for _, a := range resp.Actors {
				atespace := ""
				name := ""
				version := int64(1)
				if a.Metadata != nil {
					atespace = a.Metadata.Atespace
					name = a.Metadata.Name
					version = a.Metadata.Version
				}
				tmpl := ""
				if a.ActorTemplate != nil {
					tmpl = a.ActorTemplate.Name
				}
				state := "SUSPENDED"
				workerPod := ""
				workerIP := ""
				if a.Status != nil {
					state = a.Status.State.String()
					if a.Status.WorkerAssignment != nil {
						workerPod = a.Status.WorkerAssignment.WorkerPod
						workerIP = a.Status.WorkerAssignment.WorkerPodIp
					}
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\tv%d\n",
					atespace, name, tmpl, state, workerPod, workerIP, version)
			}
			w.Flush()
			return nil
		},
	}

	var actorResumeCmd = &cobra.Command{
		Use:   "resume <name>",
		Short: "Resume an actor on an available worker",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			res, err := c.Control.ResumeActor(context.Background(), &ateapipb.ResumeActorRequest{
				Actor: &ateapipb.ObjectRef{
					Atespace: actorAtespace,
					Name:     args[0],
				},
			})
			if err != nil {
				return err
			}
			workerPod := ""
			workerIP := ""
			if res.Actor != nil && res.Actor.Status != nil && res.Actor.Status.WorkerAssignment != nil {
				workerPod = res.Actor.Status.WorkerAssignment.WorkerPod
				workerIP = res.Actor.Status.WorkerAssignment.WorkerPodIp
			}
			fmt.Printf("actor %s/%s resumed on %s (%s)\n", actorAtespace, args[0], workerPod, workerIP)
			return nil
		},
	}

	var actorSuspendCmd = &cobra.Command{
		Use:   "suspend <name>",
		Short: "Suspend an actor and snapshot its state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			res, err := c.Control.SuspendActor(context.Background(), &ateapipb.SuspendActorRequest{
				Actor: &ateapipb.ObjectRef{
					Atespace: actorAtespace,
					Name:     args[0],
				},
			})
			if err != nil {
				return err
			}
			snap := ""
			if res.Actor != nil && res.Actor.Status != nil && res.Actor.Status.ExternalSnapshot != nil {
				snap = res.Actor.Status.ExternalSnapshot.SnapshotUri
			}
			fmt.Printf("actor %s/%s suspended (snapshot: %s)\n", actorAtespace, args[0], snap)
			return nil
		},
	}

	var actorPauseCmd = &cobra.Command{
		Use:   "pause <name>",
		Short: "Pause an actor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			_, err = c.Control.PauseActor(context.Background(), &ateapipb.PauseActorRequest{
				Actor: &ateapipb.ObjectRef{
					Atespace: actorAtespace,
					Name:     args[0],
				},
			})
			if err != nil {
				return err
			}
			fmt.Printf("actor %s/%s paused\n", actorAtespace, args[0])
			return nil
		},
	}

	var actorDeleteCmd = &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an actor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			res, err := c.Control.DeleteActor(context.Background(), &ateapipb.DeleteActorRequest{
				Actor: &ateapipb.ObjectRef{
					Atespace: actorAtespace,
					Name:     args[0],
				},
				AnyState: anyState,
			})
			if err != nil {
				return err
			}
			fmt.Printf("actor %s/%s deleted\n", res.Metadata.Atespace, res.Metadata.Name)
			return nil
		},
	}
	actorDeleteCmd.Flags().BoolVar(&anyState, "any-state", false, "Delete actor regardless of state")

	var actorLogsCmd = &cobra.Command{
		Use:   "logs <name>",
		Short: "View logs for an actor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get(fmt.Sprintf("http://localhost:8082/api/v1/logs?atespace=%s&actor=%s", actorAtespace, args[0]))
			if err != nil {
				return fmt.Errorf("failed to fetch logs: %w", err)
			}
			defer resp.Body.Close()

			var data struct {
				Logs []string `json:"logs"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&data)
			if len(data.Logs) == 0 {
				fmt.Println("(no logs recorded)")
				return nil
			}
			for _, line := range data.Logs {
				fmt.Println(line)
			}
			return nil
		},
	}

	actorCmd.AddCommand(actorCreateCmd, actorListCmd, actorResumeCmd, actorSuspendCmd, actorPauseCmd, actorDeleteCmd, actorLogsCmd)
	rootCmd.AddCommand(actorCmd)

	// --- worker subcommands ---
	workerCmd := &cobra.Command{
		Use:     "worker",
		Aliases: []string{"workers"},
		Short:   "Inspect and manage physical workers",
	}

	var workerListCmd = &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "get"},
		Short:   "List physical workers and assignments",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			resp, err := c.Control.ListWorkers(context.Background(), &ateapipb.ListWorkersRequest{})
			if err != nil {
				return err
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintln(w, "NAME\tSTATUS\tASSIGNED ACTORS\tIP")
			for _, wrk := range resp.Workers {
				name := ""
				if wrk.Metadata != nil {
					name = wrk.Metadata.Name
				}
				status := "FREE"
				actorsCount := int32(0)
				if wrk.Status != nil {
					if wrk.Status.State == ateapipb.WorkerState_WORKER_STATE_DRAINING {
						status = "DRAINING"
					} else if wrk.Status.Allocated != nil && wrk.Status.Allocated.Actors > 0 {
						status = "ASSIGNED"
						actorsCount = wrk.Status.Allocated.Actors
					}
				}
				fmt.Fprintf(w, "%s\t%s\t%d\t%s\n", name, status, actorsCount, wrk.Ip)
			}
			w.Flush()
			return nil
		},
	}

	var workerDrainCmd = &cobra.Command{
		Use:   "drain <name>",
		Short: "Drain a physical worker",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			res, err := c.Control.DrainWorker(context.Background(), &ateapipb.DrainWorkerRequest{
				Worker: &ateapipb.ObjectRef{Name: args[0]},
			})
			if err != nil {
				return err
			}
			fmt.Printf("worker %s set to draining\n", res.Metadata.Name)
			return nil
		},
	}

	workerCmd.AddCommand(workerListCmd, workerDrainCmd)
	rootCmd.AddCommand(workerCmd)

	// --- tag subcommands ---
	var tagAtespace string
	var tagActor string
	tagCmd := &cobra.Command{
		Use:     "tag",
		Aliases: []string{"tags"},
		Short:   "Manage snapshot tags",
	}
	tagCmd.PersistentFlags().StringVarP(&tagAtespace, "atespace", "a", "default", "Atespace for tag")

	var tagCreateCmd = &cobra.Command{
		Use:   "create <name>",
		Short: "Tag an actor's snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			res, err := c.Control.CreateTag(context.Background(), &ateapipb.CreateTagRequest{
				Tag: &ateapipb.Tag{
					Metadata: &ateapipb.ResourceMetadata{
						Atespace: tagAtespace,
						Name:     args[0],
					},
					Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE,
					SourceActor: &ateapipb.ObjectRef{
						Atespace: tagAtespace,
						Name:     tagActor,
					},
					Status: &ateapipb.TagStatus{
						Snapshot: &ateapipb.ExternalSnapshot{
							SnapshotUri: fmt.Sprintf("miniate-snap://%s/%s/v1", tagAtespace, tagActor),
						},
					},
				},
			})
			if err != nil {
				return err
			}
			fmt.Printf("tag %s/%s created\n", res.Metadata.Atespace, res.Metadata.Name)
			return nil
		},
	}
	tagCreateCmd.Flags().StringVar(&tagActor, "actor", "", "Actor name to tag")

	var tagListCmd = &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "get"},
		Short:   "List snapshot tags",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			resp, err := c.Control.ListTags(context.Background(), &ateapipb.ListTagsRequest{Atespace: tagAtespace})
			if err != nil {
				return err
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintln(w, "ATESPACE\tNAME\tSNAPSHOT URI")
			for _, t := range resp.Tags {
				as := ""
				name := ""
				if t.Metadata != nil {
					as = t.Metadata.Atespace
					name = t.Metadata.Name
				}
				uri := ""
				if t.Status != nil && t.Status.Snapshot != nil {
					uri = t.Status.Snapshot.SnapshotUri
				}
				fmt.Fprintf(w, "%s\t%s\t%s\n", as, name, uri)
			}
			w.Flush()
			return nil
		},
	}

	var tagDeleteCmd = &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a tag",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := client.NewClient(grpcEndpoint)
			if err != nil {
				return err
			}
			defer c.Close()

			_, err = c.Control.DeleteTag(context.Background(), &ateapipb.DeleteTagRequest{
				Tag: &ateapipb.ObjectRef{
					Atespace: tagAtespace,
					Name:     args[0],
				},
			})
			if err != nil {
				return err
			}
			fmt.Printf("tag %s/%s deleted\n", tagAtespace, args[0])
			return nil
		},
	}

	tagCmd.AddCommand(tagCreateCmd, tagListCmd, tagDeleteCmd)
	rootCmd.AddCommand(tagCmd)


	// --- version ---
	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print miniate version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("miniate %s (Substrate local developer runtime)\n", versionString)
		},
	}
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
