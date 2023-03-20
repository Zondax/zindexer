GOFMT_FILES?=$$(find . -name '*.go')

build:
	go build ./...

clean:
	go clean

gitclean:
	git clean -xfd
	git submodule foreach --recursive git clean -xfd

install_lint:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.51.0

check-modtidy:
	go mod tidy
	git diff --exit-code -- go.mod go.sum

lint:
	@echo "==> Checking that code complies with golangci-lint requirements..."
	golangci-lint --version
	golangci-lint run -E gofmt -E goconst -E gocritic --timeout 5m

fmt:
	gofmt -w -s $(GOFMT_FILES)

fmt-check:
	@echo "==> Checking that code complies with gofmt requirements..."
	gofmt -l -s $(GOFMT_FILES)

vet:
	@echo "==> Checking that code complies with go vet requirements..."
	go vet $$(go list ./...)

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

