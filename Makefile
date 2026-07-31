.PHONY: test race vet build check

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

build:
	go build -trimpath -o aipermission-backup ./cmd/aipermission-backup

check: test race vet build

