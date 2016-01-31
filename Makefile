all: query writer

query: bindir
	go build -o bin/edb-query ./src/query/ 

writer: bindir
	go build -o bin/edb-write ./src/write/

datagen: bindir
	go build -o bin/edb-fakedata ./src/fakedata

testdata:
	./bin/edb-fakedata -add 100000 -table test0

testquery:
	./bin/edb-query -table test0 -int age,f1 -op hist -group state
	

bindir:
	mkdir bin 2>/dev/null || true
      
profile: bindir
	echo "Compiling binaries with profiling enabled, use -profile flag to turn on profiling"
	go build -tags profile -o bin/edb-write ./src/write
	go build -tags profile -o bin/edb-query ./src/query
	go build -tags profile -o bin/edb-fakedata ./src/fakedata

tags: 
	ctags --languages=+Go src/lib/*.go src/query/*.go src/write/*.go

default: all

clean:
	rm ./edb-query ./edb-write

.PHONY: tags 
.PHONY: query 
.PHONY: write
.PHONY: clean

