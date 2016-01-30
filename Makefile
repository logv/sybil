all: query writer

query: 
	mkdir bin 2>/dev/null || true
	go build -o bin/edb-query ./src/query/ 

writer:
	mkdir bin 2>/dev/null || true
	go build -o bin/edb-write ./src/write/

profile:
	mkdir bin 2>/dev/null || true
	echo "Compiling binaries with profiling enabled, use -profile flag to turn on profiling"
	go build -tags profile -o bin/edb-write ./src/write
	go build -tags profile -o bin/edb-query ./src/query

tags: 
	ctags --languages=+Go src/lib/*.go src/query/*.go src/write/*.go

default: all

clean:
	rm ./edb-query ./edb-write

.PHONY: tags 
.PHONY: query 
.PHONY: write
.PHONY: clean

