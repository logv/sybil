package sybil

import "strings"
import "unicode"
import "github.com/Knetic/govaluate"

type ExpressionSpec struct {
	Int string
	Str string
	Set string
}

func ExprFields(expr string) []string {
	str_fields := make([]string, 0)
	f := ""
	for _, c := range expr {
		if c == ' ' {
			continue
		}

		if unicode.IsLetter(c) {
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

func BuildExpressions(t *Table, loadSpec *LoadSpec, exprSpec ExpressionSpec) []Expression {

	Print("BUILDING EXPRESSIONS")
	intexprs := make([]string, 0)
	expressions := make([]Expression, 0)

	if exprSpec.Int != "" {
		intexprs = strings.Split(exprSpec.Int, FLAGS.FIELD_SEPARATOR)
	}

	for _, expr := range intexprs {
		tokens := strings.Split(expr, FLAGS.FILTER_SEPARATOR)
		Print("TOKENS", tokens)
		ee, err := govaluate.NewEvaluableExpression(tokens[1])
		if err != nil {
			Print("Bad Expr:", tokens[1], err)
			continue
		}

		fields := ExprFields(tokens[1])

		field_ids := make([]int16, 0)

		for _, f := range fields {
			loadSpec.Int(f)
			field_ids = append(field_ids, t.get_key_id(f))
		}

		Print("FIELDS", fields, field_ids)

		expr := Expression{
			Name:     tokens[0],
			Expr:     *ee,
			Fields:   fields,
			name_id:  t.get_key_id(tokens[0]),
			ExprType: INT_VAL}
		t.KeyTypes[expr.name_id] = INT_VAL
		expressions = append(expressions, expr)

	}

	loadSpec.expressions = expressions

	return expressions

}
