BUILD_CMD = /usr/bin/go install
BINDIR = ./bin
GOBINDIR = ./bin
PROFILE = -tags profile

all: query writer datagen

query: bindir
	GOBIN=$(GOBINDIR) $(BUILD_CMD) $(BUILD_FLAGS) ./src/query/ 

writer: bindir ./src/write
	GOBIN=$(GOBINDIR) $(BUILD_CMD) $(BUILD_FLAGS) ./src/write/

datagen: bindir
	GOBIN=$(GOBINDIR) $(BUILD_CMD) $(BUILD_FLAGS) ./src/fakedata

testdata:
	${BINDIR}/fakedata -add 100000 -table test0

testquery:
	${BINDIR}/query -table test0 -int age,f1 -op hist -group state
	

bindir:
	mkdir ${BINDIR} 2>/dev/null || true
     

deltaencoding: export BUILD_FLAGS += -tags denc
deltaencoding: bindir
	make query
	make writer
	make datagen
profile: export BUILD_FLAGS += -tags profile
profile: bindir
	make query
	make writer
	make datagen

tags: 
	ctags --languages=+Go src/lib/*.go src/query/*.go src/write/*.go

default: all

clean:
	rm ./bin/*

.PHONY: tags 
.PHONY: query 
.PHONY: write
.PHONY: clean

