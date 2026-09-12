# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BINARY_NAME=miniate
BIN_DIR=bin

.PHONY: all build test test-cover install fmt tidy clean start stop status dashboard help

all: build test

## build: Build the miniate CLI binary
build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/miniate
	@echo "Built $(BIN_DIR)/$(BINARY_NAME)"

## install: Install miniate binary to $(GOPATH)/bin
install:
	go install ./cmd/miniate
	@echo "Installed miniate to $(shell go env GOPATH)/bin/miniate"

## test: Run all package tests
test:
	go test -v ./...

## test-cover: Run tests with coverage report
test-cover:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

## fmt: Format all Go source files
fmt:
	go fmt ./...

## tidy: Run go mod tidy to update dependencies
tidy:
	go mod tidy

## start: Start miniate local daemon in the background
start: build
	./$(BIN_DIR)/$(BINARY_NAME) start

## stop: Stop the running miniate background daemon
stop: build
	./$(BIN_DIR)/$(BINARY_NAME) stop

## status: Check the status of the local miniate cluster
status: build
	./$(BIN_DIR)/$(BINARY_NAME) status

## dashboard: Print/open the Web Dashboard URL
dashboard:
	@echo "Opening Web Dashboard: http://localhost:8082/dashboard"
	@open http://localhost:8082/dashboard 2>/dev/null || xdg-open http://localhost:8082/dashboard 2>/dev/null || true

## clean: Remove built binaries and coverage files
clean:
	rm -rf $(BIN_DIR) coverage.out

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'
