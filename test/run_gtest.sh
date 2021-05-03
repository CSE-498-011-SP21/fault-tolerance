#!/bin/bash

test_json="gtest_kvcg.json"

# Create config
echo "{"                                        >  $test_json &&
echo "    \"servers\" : ["                      >> $test_json &&
echo "      {"                                  >> $test_json &&
echo "        \"name\": \"${HOSTNAME}\","       >> $test_json &&
echo "        \"address\": \"127.0.0.1\","      >> $test_json &&
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
#ftTest.batch_mixed
#ftTest.single_logRequest
#ftTest.multi_put
#ftTest.bad_multi_put
#ftTest.batch_put
#ftTest.batch_mixed
#ftTest.bad_batch
#ftTest.unfold_requests

for test in ${testlist}; do
  ./ftTest --gtest_filter=${test}
  tres=$?
  if [ ${tres} -ne 0 ]; then
    echo "TEST FAILURE: ${test}"
    res=1
  fi
done

exit ${res}
