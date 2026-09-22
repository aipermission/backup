package docs

import (
	"os"
	"strings"
	"testing"
)

func TestCIRunsRepositoryQualityGate(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(workflow), "run: make check") {
		t.Fatal("CI must run the repository quality gate")
	}

	makefile, err := os.ReadFile("../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	text := string(makefile)
	for _, required := range []string{
		"format-check:",
		"quality:",
		"coverage:",
		"race:",
		"vet:",
		"build:",
		"check: format-check quality test coverage race vet build",
		"go run ./scripts/qualitycheck.go .",
		"minimum 65.0%",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("repository quality gate is missing %q", required)
		}
	}
}
