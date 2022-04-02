#!/bin/bash

SEED=$1
scrdir="$(dirname $0)"
cd ${scrdir}

declare -i RUN_STATUS=1

echo "time: $(date +%s)"

rm -f log.txt
./runcmsgrid.sh $SEED &> log.txt
RUN_STATUS=$?

if [[ ${RUN_STATUS} -eq 0 ]]; then
  if [[ ! -s results.top ]]; then
    RUN_STATUS=99
  fi
  echo "- RUN_STATUS: ${RUN_STATUS}"

  echo "time: $(date +%s)"
fi

if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "top file generation failed with error code ${RUN_STATUS}. Output:"
  cat log.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "SUCCESSFUL TOP FILE GENERATION"
fi
