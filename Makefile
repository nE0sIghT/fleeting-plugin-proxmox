GO ?= go
GOFMT ?= gofmt
BINARY ?= dist/fleeting-plugin-proxmox
VERSION ?= $(shell tag=$$(git describe --tags --exact-match 2>/dev/null || true); if [ -n "$$tag" ]; then printf '%s' "$${tag#v}"; else printf 'dev-%s' "$$(git rev-parse --short HEAD)"; fi)
REVISION ?= $(shell git rev-parse HEAD)
REFERENCE ?= $(shell tag=$$(git describe --tags --exact-match 2>/dev/null || true); if [ -n "$$tag" ]; then printf '%s' "$$tag"; else git rev-parse --abbrev-ref HEAD; fi)
BUILT ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS ?= -s -w \
	-X 'gitlab.com/gitlab-org/fleeting/plugins/proxmox.VERSION=$(VERSION)' \
	-X 'gitlab.com/gitlab-org/fleeting/plugins/proxmox.REVISION=$(REVISION)' \
	-X 'gitlab.com/gitlab-org/fleeting/plugins/proxmox.REFERENCE=$(REFERENCE)' \
	-X 'gitlab.com/gitlab-org/fleeting/plugins/proxmox.BUILT=$(BUILT)'

.PHONY: build test fmt clean

build:
	mkdir -p $(dir $(BINARY))
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/fleeting-plugin-proxmox

test:
	$(GO) test ./...

fmt:
	$(GOFMT) -w $$(find . -name '*.go' -not -path './dist/*')

clean:
	rm -rf dist
