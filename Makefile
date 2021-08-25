build:
	go build ./...

clean:
	go clean

gitclean:
	git clean -xfd
	git submodule foreach --recursive git clean -xfd

install_lint:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.35.2

check-modtidy:
	go mod tidy
	git diff --exit-code -- go.mod go.sum

lint:
	golangci-lint --version
	golangci-lint run -E gofmt -E gosec -E goconst -E gocritic
#   golangci-lint run -E stylecheck -E gosec -E goconst -E godox -E gocritic

test: build
	go test -race ./...

install-deps:
	go get ./...

test-mongo: install-deps
	cd ./connections/database && go test
