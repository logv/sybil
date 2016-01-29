all: tags query writer

tags: 
	ctags --languages=+Go src/lib/*.go src/query/*.go src/write/*.go > /dev/null

query: 
	go build -o edb-query ./src/query/ 

writer:
	go build -o edb-write ./src/write/

default: all

.PHONY: tags 
.PHONY: query 
.PHONY: write

