package velum_test

import (
	"strings"
	"testing"

	"github.com/axkit/velum"
)

func TestArgAsQuestionMark(t *testing.T) {
	for _, i := range []int{1, 5, 99} {
		if got := velum.ArgAsQuestionMark(i); got != "?" {
			t.Errorf("ArgAsQuestionMark(%d) = %q, want ?", i, got)
		}
	}
}

func TestIsTagOptionExist(t *testing.T) {
	tests := []struct {
		tag, opt string
		want     bool
	}{
		{"pk", "pk", true},                    // exact match
		{"pk,gen=serial", "pk", true},         // at prefix
		{"gen=serial,pk", "pk", true},         // at suffix
		{"gen=serial,pk,name=id", "pk", true}, // in middle
		{"gen=serial", "pk", false},           // not present
		{"pk_extra", "pk", false},             // partial match is not a token
		{"", "pk", false},                     // empty tag
	}
	for _, tt := range tests {
		if got := velum.IsTagOptionExist(tt.tag, tt.opt); got != tt.want {
			t.Errorf("IsTagOptionExist(%q, %q) = %v, want %v", tt.tag, tt.opt, got, tt.want)
		}
	}
}

// TestToSnakeCase_NameTagWithComma covers the branch where the name= option
// is followed by additional comma-separated tag options.
func TestToSnakeCase_NameTagWithComma(t *testing.T) {
	got := velum.ToSnakeCase("FirstName", "name=first_name,pk")
	if got != "first_name" {
		t.Errorf("ToSnakeCase name= with comma = %q, want %q", got, "first_name")
	}
}

func TestTable_Getters(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")

	created := tbl.Created()
	if len(created) == 0 {
		t.Error("Created() returned empty slice")
	}

	_ = tbl.Updated()

	deleted := tbl.Deleted()
	if len(deleted) == 0 {
		t.Error("Deleted() returned empty slice")
	}

	version := tbl.Version()
	if version == nil {
		t.Error("Version() returned nil")
	}

	_ = tbl.FriendlySequence()

	fn := tbl.ArgNumerator()
	if fn == nil {
		t.Fatal("ArgNumerator() returned nil")
	}
	if got := fn(1); got != "$1" {
		t.Errorf("ArgNumerator()(1) = %q, want $1", got)
	}

	_ = tbl.ScopeContainer()
}

func TestTable_Scope_Valid(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	s := tbl.Scope("age")
	if s == "" {
		t.Error("Scope(\"age\") returned empty string")
	}
}

func TestTable_Scope_Panic(t *testing.T) {
	tbl := velum.NewTable[CustomerSerial]("customers")
	defer func() {
		if r := recover(); r == nil {
			t.Error("Scope with invalid name: expected panic, got none")
		}
	}()
	tbl.Scope("no_such_scope_xyz")
}

func TestByClauses(t *testing.T) {
	opt := velum.ByClauses("WHERE id=$1")
	if got := opt(); got != "WHERE id=$1" {
		t.Errorf("ByClauses()() = %q, want %q", got, "WHERE id=$1")
	}
}

func TestDataset_Statement(t *testing.T) {
	stmt := "SELECT id, first_name, age FROM customers_pk_serial /*WHERE*/"
	ds := velum.NewDataset[CustomerSerial](stmt)
	if got := ds.Statement(); got != stmt {
		t.Errorf("Statement() = %q, want %q", got, stmt)
	}
}

func TestDataset_Command(t *testing.T) {
	ds := velum.NewDataset[CustomerSerial]("SELECT id, first_name, age FROM t")
	cmd := ds.Command("ORDER BY id")
	_ = cmd
}

func TestDataset_WithTailClause(t *testing.T) {
	ds := velum.NewDataset[CustomerSerial]("SELECT id, first_name, age FROM t")
	dq := ds.WithTailClause("ORDER BY id DESC LIMIT 10")
	_ = dq
}

func TestDataset_WithDatasetTag(t *testing.T) {
	ds := velum.NewDataset[CustomerSerial]("SELECT id FROM t", velum.WithDatasetTag("dbw"))
	_ = ds
}

func TestDataset_WithDatasetColumnNameBuilder(t *testing.T) {
	ds := velum.NewDataset[CustomerSerial]("SELECT id FROM t",
		velum.WithDatasetColumnNameBuilder(func(attr, tag string) string {
			return strings.ToLower(attr)
		}),
	)
	_ = ds
}
