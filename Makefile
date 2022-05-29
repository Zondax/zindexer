build:
	go build ./...

clean:
	go clean

gitclean:
	git clean -xfd
	git submodule foreach --recursive git clean -xfd

install_lint:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v1.45.2

check-modtidy:
	go mod tidy
	git diff --exit-code -- go.mod go.sum

lint:
	golangci-lint --version
	golangci-lint run -E gofmt -E gosec -E goconst -E gocritic
#   golangci-lint run -E stylecheck -E gosec -E goconst -E godox -E gocritic

test: build
	go test -race ./...

test-database: build
	go test ./components/connections/database/... -v

test-integration: build
	go test -v ./indexer/tests/...

test-components: build
	 go test -v ./components/...

# Docker
test-database-up:
	docker-compose -f ./indexer/tests/docker-compose.yml up -d

test-database-services:
	docker-compose -f tests_docker/test_database.yml up --abort-on-container-exit

