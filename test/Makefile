CXX=gcc
CPPFLAGS=-lstdc++ -std=c++0x -lfabric -O3 -I ../include -I ../common/include

SOURCES := $(shell ls ../src/*.cc)

APPS := $(shell echo unittest_fault_tolerance)

.PHONY: all
all: $(APPS)

#default rule for all apps
% : %.cc
	$(CXX) $(CPPFLAGS) $(SOURCES) $^ -o $@

.PHONY: clean
clean:
	rm -f $(APPS)
