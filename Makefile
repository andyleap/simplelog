# simplelog build & codegen.
#
# Toolchain (one-time):
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install storj.io/drpc/cmd/protoc-gen-go-drpc@latest
#   # plus a protoc binary on PATH (https://github.com/protocolbuffers/protobuf/releases)

GOBIN := $(shell go env GOPATH)/bin
export PATH := $(GOBIN):$(PATH)

.PHONY: all proto build test agent manager

all: build

proto:
	protoc --proto_path=proto \
	  --go_out=. --go_opt=module=github.com/andyleap/simplelog \
	  --go-drpc_out=. --go-drpc_opt=module=github.com/andyleap/simplelog \
	  proto/ingest.proto

build:
	go build ./...

test:
	go test ./...

agent:
	CGO_ENABLED=0 go build -o bin/agent ./cmd/agent

manager:
	CGO_ENABLED=0 go build -o bin/manager ./cmd/manager
