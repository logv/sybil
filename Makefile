BUILD_CMD = /usr/bin/go install
BINDIR = ./bin
GOBINDIR = ./bin
PROFILE = -tags profile


all: query ingest digest

query: bindir
	GOBIN=$(GOBINDIR) $(BUILD_CMD) $(BUILD_FLAGS) ./src/query/ 

digest: bindir 
	GOBIN=$(GOBINDIR) $(BUILD_CMD) $(BUILD_FLAGS) ./src/digest/

ingest: bindir 
	GOBIN=$(GOBINDIR) $(BUILD_CMD) $(BUILD_FLAGS) ./src/ingest/

fake-data: fake-uptime

fake-uptime:
	python scripts/fakedata/host_generator.py 100000 | ./bin/ingest -table uptime
	./bin/digest -table uptime

testquery:
	${BINDIR}/query -table test0 -int age,f1 -op hist -group state
	

bindir:
	mkdir ${BINDIR} 2>/dev/null || true
     

nodeltaencoding: export BUILD_FLAGS += -tags denc
nodeltaencoding: bindir
	make all

profile: export BUILD_FLAGS += -tags profile
profile: bindir
	make all

tags: 
	ctags --languages=+Go src/lib/*.go src/query/*.go src/ingest/*.go

default: all

clean:
	rm ./bin/*

.PHONY: tags 
.PHONY: query 
.PHONY: ingest
.PHONY: clean

