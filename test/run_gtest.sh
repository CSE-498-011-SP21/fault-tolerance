#!/bin/bash

test_json="gtest_kvcg.json"

# Create config
echo "{"                                        >  $test_json &&
echo "    \"servers\" : ["                      >> $test_json &&
echo "      {"                                  >> $test_json &&
echo "        \"name\": \"${HOSTNAME}\","       >> $test_json &&
echo "        \"minKey\": 0,"                   >> $test_json &&
echo "        \"maxKey\": 1000,"                >> $test_json &&
echo "        \"backups\": [\"${HOSTNAME}\"]"   >> $test_json &&
echo "      }"                                  >> $test_json &&
echo "    ]"                                    >> $test_json &&
echo "}"                                        >> $test_json
if [ $? -ne 0 ]; then exit 1; fi

# Build GTest
make clean ftTest || exit $?

# Run GTest
res=0
testlist="ftTest.batch_mixed"
for test in ${testlist}; do
  ./ftTest --gtest_filter=${test}
  tres=$?
  if [ ${tres} -ne 0 ]; then
    echo "TEST FAILURE: ${test}"
    res=1
  fi
done

exit ${res}
