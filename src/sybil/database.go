package sybil

// Database is a collection of tables AKA datasets.
type Database struct {
	Dir string
}

func NewDatabase(dir string) (*Database, error) {
	return &Database{Dir: dir}, nil
	// TODO: fs sanity checks?
}

func (db *Database) ListTables() ([]string, error) {
	// TODO: ListTables uses globals
	return ListTables(), nil
}
