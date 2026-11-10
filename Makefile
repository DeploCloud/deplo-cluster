# The Deplo cluster binary: a cluster leader, or with `relay` a cluster relay.
#
#   make build -> ./bin/deplo-cluster (static, this platform)
#   make test  -> go test ./...
#   make vet   -> go vet ./...
#   make fmt   -> gofmt

# The version comes from the git tag, leading v stripped, like the release workflow.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/^v//' || echo dev)
LDFLAGS := -X github.com/DeploCloud/deplo-cluster/internal/cluster.Version=$(VERSION)

.PHONY: build test vet fmt clean

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/deplo-cluster .

test:
	CGO_ENABLED=0 go test ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -rf bin dist
