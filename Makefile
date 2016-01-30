all: query writer

query: 
	go build -o edb-query ./src/query/ 

writer:
	go build -o edb-write ./src/write/

profile:
	go build -tags profile -o edb-write ./src/write
	go build -tags profile -o edb-query ./src/query

tags: 
	ctags --languages=+Go src/lib/*.go src/query/*.go src/write/*.go

default: all

clean:
	rm ./edb-query ./edb-write

.PHONY: tags 
.PHONY: query 
.PHONY: write
.PHONY: clean

