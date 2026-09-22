package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	maxSourceLines   = 600
	maxFunctionLines = 140
)

type violation struct {
	path    string
	line    int
	message string
}

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	violations, err := inspect(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(violations) == 0 {
		fmt.Printf("quality budgets passed: source <= %d lines, functions <= %d lines\n", maxSourceLines, maxFunctionLines)
		return
	}
	for _, item := range violations {
		fmt.Fprintf(os.Stderr, "%s:%d: %s\n", item.path, item.line, item.message)
	}
	os.Exit(1)
}

func inspect(root string) ([]violation, error) {
	set := token.NewFileSet()
	var violations []violation
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") || filepath.Base(path) == "qualitycheck.go" {
			return nil
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		lineCount := countLines(source)
		if lineCount > maxSourceLines {
			violations = append(violations, violation{relative, 1, fmt.Sprintf("source file has %d lines; maximum is %d", lineCount, maxSourceLines)})
		}
		file, err := parser.ParseFile(set, path, source, 0)
		if err != nil {
			return fmt.Errorf("parse %s: %w", relative, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			start := set.Position(function.Pos()).Line
			end := set.Position(function.End()).Line
			if length := end - start + 1; length > maxFunctionLines {
				violations = append(violations, violation{relative, start, fmt.Sprintf("function %s has %d lines; maximum is %d", function.Name.Name, length, maxFunctionLines)})
			}
		}
		return nil
	})
	sort.Slice(violations, func(i, j int) bool {
		if violations[i].path == violations[j].path {
			return violations[i].line < violations[j].line
		}
		return violations[i].path < violations[j].path
	})
	return violations, err
}

func countLines(source []byte) int {
	if len(source) == 0 {
		return 0
	}
	lines := 1
	for _, value := range source {
		if value == '\n' {
			lines++
		}
	}
	if source[len(source)-1] == '\n' {
		lines--
	}
	return lines
}
