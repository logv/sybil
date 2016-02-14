BUILD_CMD = /usr/bin/go install
BINDIR = ./bin
GOBINDIR = ./bin
PROFILE = -tags profile


all: sybil

sybil: bindir
	GOBIN=$(GOBINDIR) $(BUILD_CMD) $(BUILD_FLAGS) ./

fake-data: fake-uptime fake-people

fake-people:
	python scripts/fakedata/people_generator.py 50000 | ./bin/sybil ingest -table people
	./bin/sybil digest -table people

fake-uptime:
	python scripts/fakedata/host_generator.py 100000 | ./bin/sybil ingest -table uptime
	./bin/sybil digest -table uptime

testquery:
	${BINDIR}/sybil query -table people -int age,f1 -op hist -group state
	

bindir:
	mkdir ${BINDIR} 2>/dev/null || true

test:
	go test ./src/lib/test

testv:
	go test ./src/lib/test -v
     

nodeltaencoding: export BUILD_FLAGS += -tags denc
nodeltaencoding: bindir
	make all

profile: export BUILD_FLAGS += -tags profile
profile: bindir
	make all

tags: 
	ctags --languages=+Go src/lib/*.go

default: all

clean:
	rm ./bin/*

.PHONY: tags 
.PHONY: query 
.PHONY: ingest
.PHONY: clean

