
# Constants to override at build time
PACKAGE := github.com/zondax/zindexer
REVISION := $(shell git rev-parse --short HEAD)
APPNAME := zindexer

build:
	go build -ldflags "-X $(PACKAGE).GitRevision=$(REVISION)" -o $(APPNAME) ./...

clean:
	go clean

gitclean:
	git clean -xfd
	git submodule foreach --recursive git clean -xfd

install_lint:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.31.0

check-modtidy:
	go mod tidy
	git diff --exit-code -- go.mod go.sum

lint:
	golangci-lint --version
	golangci-lint run -E gofmt -E gosec -E goconst -E gocritic
#   golangci-lint run -E stylecheck -E gosec -E goconst -E godox -E gocritic

test: build
	go test -race ./tests
