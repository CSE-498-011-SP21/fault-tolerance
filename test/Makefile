CXX=g++
CPPFLAGS=\
-lstdc++ -std=c++17 \
-lfabric -lboost_system \
-O3 -g -pthread

CPPINCLUDES=\
-I ../include \
-I ../lib/common/include \
-I ../lib/network-layer/commonAPI/include -I ../lib/network-layer/fabricBased/include

SOURCES := $(shell ls ../src/*.cc)

APPS := $(shell echo unittest_fault_tolerance)

.PHONY: all
all: $(APPS)

ftTest : ftTest.cc
	$(CXX) $(CPPINCLUDES) $(SOURCES) $^ -o $@ $(CPPFLAGS) -lgtest_main  -lgtest -DLOOPBACK

#default rule for all apps
% : %.cc
	$(CXX) $(CPPINCLUDES) $(SOURCES) $^ -o $@ $(CPPFLAGS)

.PHONY: clean
clean:
	rm -f $(APPS) ftTest
