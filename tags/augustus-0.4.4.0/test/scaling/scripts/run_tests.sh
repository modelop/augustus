#/bin/bash

TESTS="1k 1m 100m 1g 4g 8g 16g"

CONFIG_DIR=../configs
UTIL_DIR=../util
OUT_DIR=../_out

# Clean up from previous runs
rm -rf ${OUT_DIR}
mkdir ${OUT_DIR}

for test in ${TESTS} ;
  do
    echo "Running ${test} test"
    # File-bound Scoring
    /usr/bin/time AugustusPMMLConsumer -c ${CONFIG_DIR}/${test}-config.xml
    # Interactive Scoring
    /usr/bin/time python ${UTIL_DIR}/event-tester.py ${test}
done
