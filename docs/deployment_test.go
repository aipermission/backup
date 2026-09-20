package docs

import (
	"os"
	"strings"
	"testing"
)

func TestUpgradeRollbackRequiresFullVolumeRestore(t *testing.T) {
	content, err := os.ReadFile("deployment.md")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(strings.Fields(string(content)), " ")
	for _, required := range []string{
		"restore the **entire service volume**",
		"Never restore `metadata.db` alone",
		"a matching full-volume snapshot is mandatory",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("deployment rollback contract is missing %q", required)
		}
	}
	if strings.Contains(text, "replace `metadata.db` with the matching pre-migration snapshot") {
		t.Fatal("deployment guide still recommends unsafe metadata-only rollback")
	}
}
