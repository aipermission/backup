package docs

import (
	"os"
	"strings"
	"testing"
)

func TestImagePublicationVerifiesSourceBeforeRegistryMutation(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/publish-image.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(workflow)
	for _, required := range []string{
		"needs: verify",
		"git merge-base --is-ancestor",
		"run: make check",
		"image --exit-code 1 --severity HIGH,CRITICAL",
		"bash scripts/publish-image.sh",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("publish workflow is missing %q", required)
		}
	}
	if strings.Contains(text, "continue-on-error: true") {
		t.Fatal("publish workflow must not make release security checks advisory")
	}
	if strings.Index(text, "Scan exact candidate image") > strings.Index(text, "Log in to GitHub Container Registry") {
		t.Fatal("publish workflow authenticates to the registry before candidate verification")
	}
}

func TestContinuousIntegrationBlocksVulnerableImages(t *testing.T) {
	workflow, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(workflow)
	if !strings.Contains(text, "image --exit-code 1 --severity HIGH,CRITICAL") {
		t.Fatal("CI image scan must fail on high or critical vulnerabilities")
	}
	if strings.Contains(text, "continue-on-error: true") {
		t.Fatal("CI image scan must not be advisory")
	}
}
