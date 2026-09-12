.PHONY: build check fmt
build:
	mkdir -p bin
	CGO_ENABLED=0 go build -trimpath -o bin/notifier ./cmd/notifier
	CGO_ENABLED=0 go build -trimpath -o bin/notify-admin ./cmd/notify-admin
	CGO_ENABLED=0 go build -trimpath -o bin/mock-target ./cmd/mock-target
	CGO_ENABLED=0 go build -trimpath -o bin/healthcheck ./cmd/healthcheck
check:
	go vet ./...
	go mod verify
	git diff --check
fmt:
	gofmt -w cmd internal
