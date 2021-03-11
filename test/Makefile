CXX=g++
CPPFLAGS=\
-lstdc++ -std=c++14 \
-lfabric -lboost_system \
-lfmt -DSPDLOG_FMT_EXTERNAL=1 \
-O3 -g -pthread

CPPINCLUDES=\
-I ../include \
-I ../common/include \
-I ../network-layer/commonAPI/include -I ../network-layer/fabricBased/include

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
