package sybil

import "log"
import "strings"
import "unicode"
import "github.com/google/cel-go/cel"
import "github.com/google/cel-go/checker/decls"

import exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

type ExpressionSpec struct {
	Int string
	Str string
	Set string
}

type Expression struct {
	Name     string
	Expr     cel.Program
	Fields   []string
	FieldIds []int16
	name_id  int16
	ExprType int8
}

func ExprFields(expr string) []string {
	str_fields := make([]string, 0)
	f := ""
	for _, c := range expr {
		if c == ' ' {
			continue
		}

		if unicode.IsLetter(c) || c == '_' || c == '.' {
			f += string(c)
		} else {
			if f != "" {
				str_fields = append(str_fields, f)
			}

			f = ""

		}

	}

	if f != "" {
		str_fields = append(str_fields, f)
	}

	return str_fields
}

func MakeExpression(t *Table, loadSpec *LoadSpec, expr string, expr_type int8) *Expression {
	tokens := strings.SplitN(expr, FLAGS.FILTER_SEPARATOR, 2)

	ds := make([]*exprpb.Decl, 0)
	for f, tp := range t.KeyTypes {
		fname := t.get_string_for_key(int(f))
		if tp == INT_VAL {
			ds = append(ds, decls.NewIdent(fname, decls.Int, nil))
		}

		if tp == STR_VAL {
			ds = append(ds, decls.NewIdent(fname, decls.String, nil))

		}
	}

	env, err := cel.NewEnv(cel.Declarations(ds...))

	parsed, issues := env.Parse(tokens[1])
	if issues != nil && issues.Err() != nil {
		log.Fatalf("parse error: %s", issues.Err())
	}
	checked, issues := env.Check(parsed)
	if issues != nil && issues.Err() != nil {
		log.Fatalf("type-check error: %s", issues.Err())
	}
	Print("CHECKED", checked, checked.ResultType())
	prg, err := env.Program(checked)
	if err != nil {
		log.Fatalf("program construction error: %s", err)
	}

	fields := ExprFields(tokens[1])

	field_ids := make([]int16, 0)
	for _, f := range fields {
		// properly evaluate fields later
		loadSpec.Int(f)
		field_ids = append(field_ids, t.get_key_id(f))
	}

	ex := Expression{
		Name:     tokens[0],
		Expr:     prg,
		Fields:   fields,
		FieldIds: field_ids,
		name_id:  t.get_key_id(tokens[0]),
		ExprType: expr_type}
	t.KeyTypes[ex.name_id] = expr_type
	Print("EX", ex)

	return &ex
}

func BuildExpressions(t *Table, loadSpec *LoadSpec, exprSpec ExpressionSpec) []Expression {
	Print("BUILDING EXPRESSIONS")
	expressions := make([]Expression, 0)

	if exprSpec.Int != "" {
		intexprs := strings.Split(exprSpec.Int, FLAGS.FIELD_SEPARATOR)
		for _, expr := range intexprs {
			ex := MakeExpression(t, loadSpec, expr, INT_VAL)
			if ex != nil {
				expressions = append(expressions, *ex)
			}
		}
	}

	if exprSpec.Str != "" {
		strexprs := strings.Split(exprSpec.Str, FLAGS.FIELD_SEPARATOR)
		for _, expr := range strexprs {
			ex := MakeExpression(t, loadSpec, expr, STR_VAL)
			if ex != nil {
				expressions = append(expressions, *ex)
			}
		}
	}

	Print("EXPR", expressions)

	loadSpec.expressions = expressions

	return expressions

}
