.PHONY: format-check quality test coverage race vet build check

format-check:
	@files="$$(find . -type f -name '*.go' -not -path './vendor/*' -not -path './.git/*')"; \
	unformatted="$$(gofmt -l $$files)"; \
	if [ -n "$$unformatted" ]; then \
		echo "Go files require gofmt:" >&2; \
		echo "$$unformatted" >&2; \
		exit 1; \
	fi

quality:
	go run ./scripts/qualitycheck.go .

test:
	go test ./...

coverage:
	@profile="$$(mktemp)"; \
	trap 'rm -f "$$profile"' EXIT; \
	go test -coverprofile="$$profile" ./...; \
	total="$$(go tool cover -func="$$profile" | awk '/^total:/ {gsub(/%/, "", $$3); print $$3}')"; \
	awk -v total="$$total" 'BEGIN { if (total + 0 < 65.0) { printf "coverage %.1f%% is below 65.0%%\n", total; exit 1 } }'; \
	printf 'aggregate coverage: %s%% (minimum 65.0%%)\n' "$$total"

race:
	go test -race ./...

vet:
	go vet ./...

build:
	go build -trimpath -o aipermission-backup ./cmd/aipermission-backup

check: format-check quality test coverage race vet build
