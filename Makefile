GOBIN=$(shell which go)
BUILD_CMD = ${GOBIN} install
BINDIR = ./bin
GOBINDIR = `readlink -f ./bin`
PROFILE = -tags profile
LUA = -tags lua
ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
LDFLAGS=-all-static
GO_FLAGS=--ldflags '-extldflags "-static"'


all: sybil

deps:
	${GOBIN} get -d -v -t ./...

sybil: bindir
	GOBIN=$(GOBINDIR) $(BUILD_CMD) $(GO_FLAGS) $(BUILD_FLAGS) ./

fake-data: fake-uptime

fake-people:
	python scripts/fakedata/people_generator.py 50000 | ./bin/sybil ingest -table people
	./bin/sybil digest -table people

fake-uptime:
	python scripts/fakedata/host_generator.py 1000000 | ./bin/sybil ingest -table uptime
	./bin/sybil digest -table uptime

testquery:
	${BINDIR}/sybil query -table people -int age,f1 -op hist -group state


bindir:
	mkdir ${BINDIR} 2>/dev/null || true

test:
	${GOBIN} test ./src/sybil/ -race -v

lint:
	gometalinter -t --disable-all \
		--enable=vet \
		--enable=megacheck \
		--deadline=3m ./... 2>&1

testv:
	${GOBIN} test ./src/sybil/ -race -v -debug

coverage:
	${GOBIN} test -covermode atomic -coverprofile cover.out ./src/sybil
	sed -i "s|_${ROOT_DIR}|.|"	cover.out
	${GOBIN} tool cover -html=cover.out -o cover.html


hdrhist: export BUILD_FLAGS += -tags hdrhist
hdrhist: bindir
	make all

nodeltaencoding: export BUILD_FLAGS += -tags denc
nodeltaencoding: bindir
	make all

profile: export BUILD_FLAGS += -tags profile
profile: bindir
	make all

luajit: export BUILD_FLAGS += -tags luajit
luajit: bindir
	make all

tags:
	ctags --languages=+Go src/sybil/*.go
	starscope -e cscope
	starscope -e ctags

default: all

clean:
	rm ./bin/*

fuzz: bin/cmd-fuzz-query.zip
	go-fuzz -bin=./bin/cmd-fuzz-query.zip -workdir=workdir/query

fuzzv: bin/cmd-fuzz-query.zip
	FUZZDEBUG=1 go-fuzz -bin=./bin/cmd-fuzz-query.zip -workdir=workdir/query -testoutput -procs 1

fuzzcoverage:
	${GOBIN} test -tags gofuzz -coverpkg ./... -covermode atomic -coverprofile fuzzcover.out ./src/cmd
	sed -i "s|_${ROOT_DIR}|.|"	fuzzcover.out
	${GOBIN} tool cover -html=fuzzcover.out -o fuzzcover.html

cleanfuzz:
	rm -f ./bin/cmd-fuzz*

bin/cmd-fuzz-query.zip:
	go-fuzz-build -func FuzzQuery -o bin/cmd-fuzz-query.zip github.com/logv/sybil/src/cmd

.PHONY: tags
.PHONY: query
.PHONY: ingest
.PHONY: clean
.PHONY: fuzz
.PHONY: fuzzv
.PHONY: cleanfuzz

