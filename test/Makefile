CXX=g++
CPPFLAGS=-lstdc++ -std=c++0x -O3 -lboost_system -DBOOST_SYSTEM_NO_DEPRECATED -g -pthread
CPPINCLUDES=-I ../include -I ../common/include

SOURCES := $(shell ls ../src/*.cc)

APPS := $(shell echo unittest_fault_tolerance)

.PHONY: all
all: $(APPS)

#default rule for all apps
% : %.cc
	$(CXX) $(CPPFLAGS) $(CPPINCLUDES) $(SOURCES) $^ -o $@

.PHONY: clean
clean:
	rm -f $(APPS)
