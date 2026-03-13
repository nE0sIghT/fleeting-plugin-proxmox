GO ?= go
GOFMT ?= gofmt
BINARY ?= dist/fleeting-plugin-proxmox

.PHONY: build test fmt clean

build:
	mkdir -p $(dir $(BINARY))
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -o $(BINARY) ./cmd/fleeting-plugin-proxmox

test:
	$(GO) test ./...

fmt:
	$(GOFMT) -w $$(find . -name '*.go' -not -path './dist/*')

clean:
	rm -rf dist
