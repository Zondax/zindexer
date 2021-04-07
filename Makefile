DOCKERFILE_INDEXER=docker/Dockerfile-indexer
DOCKER_IMAGE_INDEXER=zondax/indexer-example:latest

# Constants to override at build time
PACKAGE := github.com/zondax/zindexer-example
GITVERSION := $(shell go list -m all | grep $(PACKAGE) | awk '{print $$2}')
GITREVISION := $(shell git rev-parse --short HEAD)

ifeq ($(GITVERSION),)
GITVERSION := v0.0
endif

build:
	go build -ldflags "-X zindexer.GitRevision=$(GITREVISION) -X zindexer.GitVersion=$(GITVERSION)" -o zindexer-example ./cmd/zindexer-example

clean:
	go clean

clean-git:
	git clean -xfd
	git submodule foreach --recursive git clean -xfd

install-poetry:
	poetry install

install-lint:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.35.2

check-modtidy:
	go mod tidy
	git diff --exit-code -- go.mod go.sum

lint:
	golangci-lint --version
	golangci-lint run -E gofmt -E gosec -E goconst -E gocritic

lint-todo:
	golangci-lint run -E stylecheck -E gosec -E goconst -E godox -E gocritic

test: build
	go test -race -p 1 ./common/...

test-integration:
	go test -race -p 1 -v -tags=integration ./...

###############################
# DOCKER

build-docker:
	docker build -f $(DOCKERFILE_INDEXER) -t $(DOCKER_IMAGE_INDEXER) .
