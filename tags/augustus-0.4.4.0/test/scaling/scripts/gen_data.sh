#!/bin/bash

# Builds 7 data files ranging from 1KB to 16GB in size.
# These are used for the different size scale-up tests.
# If this takes too long to run, just remove the larger files from the list
# below as well as in the from the actual script which runs them

DATA_FILES="10|1k.csv 10000|1m.csv 1000000|100m.csv 10000000|1g.csv 40000000|4g.csv 80000000|8g.csv 160000000|16g.csv"

# Clean up from previous runs
DATA_DIR=../_data
rm -rf ${DATA_DIR}
mkdir ${DATA_DIR}

for file in ${DATA_FILES} ;
  do
      name=`echo ${file} | awk -F'|' '{print $2}'`
      num_records=`echo ${file} | awk -F'|' '{print $1}'`

      echo "Generating ${num_records} records for ${name}"

      python ../util/data_generator.py -m 9 -D 30 -O ${DATA_DIR}/ -o ${name} ${num_records}
done

echo "Completed $0, check for data files in ${DATA_DIR}"
