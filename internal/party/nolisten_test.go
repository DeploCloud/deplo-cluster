package party

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// A cluster party never accepts an incoming connection (ADR-0044): nothing in this package may listen.
func TestPartyNeverListens(t *testing.T) {
	fset := token.NewFileSet()
	files, err := parser.ParseDir(fset, ".", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, pkg := range files {
		for name, f := range pkg.Files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if strings.HasPrefix(sel.Sel.Name, "Listen") || sel.Sel.Name == "Serve" || sel.Sel.Name == "ServeTLS" {
						t.Errorf("%s: %s listens", fset.Position(sel.Pos()), sel.Sel.Name)
					}
				}
				return true
			})
		}
	}
}
