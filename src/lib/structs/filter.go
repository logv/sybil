package structs

type Filter interface {
	Filter(*Record) bool
}
