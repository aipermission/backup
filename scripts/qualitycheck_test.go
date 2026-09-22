package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInspectAcceptsFilesWithinBudgets(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "small.go", "package fixture\n\nfunc small() {}\n")

	violations, err := inspect(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("unexpected violations: %#v", violations)
	}
}

func TestInspectReportsSourceAndFunctionBudgets(t *testing.T) {
	root := t.TempDir()
	var source strings.Builder
	source.WriteString("package fixture\n\nfunc oversized() {\n")
	for index := 0; index < maxSourceLines; index++ {
		fmt.Fprintf(&source, "_ = %d\n", index)
	}
	source.WriteString("}\n")
	writeFixture(t, root, "oversized.go", source.String())

	violations, err := inspect(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 2 {
		t.Fatalf("violations = %#v, want source and function violations", violations)
	}
	if !strings.Contains(violations[0].message, "source file") || !strings.Contains(violations[1].message, "function oversized") {
		t.Fatalf("unexpected violations: %#v", violations)
	}
}

func TestInspectIgnoresTestsAndHiddenDirectories(t *testing.T) {
	root := t.TempDir()
	large := "package fixture\n" + strings.Repeat("\n", maxSourceLines+1)
	writeFixture(t, root, "fixture_test.go", large)
	writeFixture(t, root, filepath.Join(".cache", "generated.go"), large)

	violations, err := inspect(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) != 0 {
		t.Fatalf("ignored files produced violations: %#v", violations)
	}
}

func writeFixture(t *testing.T, root, name, content string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}
