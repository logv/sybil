package sybil

import "strings"
import "unicode"
import "github.com/Knetic/govaluate"

type ExpressionSpec struct {
	Int string
	Str string
	Set string
}

type Expression struct {
	Name     string
	Expr     govaluate.EvaluableExpression
	Fields   []string
	FieldIds []int16
	name_id  int16
	ExprType int8
}

func ExprFields(t *Table, expr string) []string {
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
				_, ok := t.KeyTable[f]
				if ok {
					str_fields = append(str_fields, f)
				}
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
	ee, err := govaluate.NewEvaluableExpression(tokens[1])
	if err != nil {
		Print("Bad Expr:", tokens[1], err)
		return nil
	}

	fields := ExprFields(t, tokens[1])

	field_ids := make([]int16, 0)
	for _, f := range fields {
		f_id := t.get_key_id(f)
		t := t.KeyTypes[f_id]
		if t == INT_VAL {
			loadSpec.Int(f)
		} else if t == STR_VAL {
			loadSpec.Str(f)

		}
		field_ids = append(field_ids, f_id)
	}

	ex := Expression{
		Name:     tokens[0],
		Expr:     *ee,
		Fields:   fields,
		FieldIds: field_ids,
		name_id:  t.get_key_id(tokens[0]),
		ExprType: expr_type}
	t.KeyTypes[ex.name_id] = expr_type

	return &ex
}

func BuildExpressions(t *Table, loadSpec *LoadSpec, exprSpec ExpressionSpec) []Expression {
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

	loadSpec.expressions = expressions

	return expressions

}
