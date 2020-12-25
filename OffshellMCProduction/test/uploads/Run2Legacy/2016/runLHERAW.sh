#!/bin/bash

NEVTS=$1
SEED=$2
NCPUS=$3

declare -i RUN_STATUS=1
declare -i LHE_SEED=${SEED}
declare -i LHE_ITER=0
while [[ ${RUN_STATUS} -ne 0 ]]; do
  echo "Grid pack iteration ${LHE_ITER} with seed ${LHE_SEED}"
  echo "time: $(date +%s)"

  ./runcmsgrid.sh ${NEVTS} ${LHE_SEED} ${NCPUS}
  RUN_STATUS=$?

  if [[ ${RUN_STATUS} -eq 0 ]]; then
    if [[ ! -s cmsgrid_final.lhe ]]; then
      RUN_STATUS=99
    elif [[ $(tail -10 cmsgrid_final.lhe | grep -e '</LesHouchesEvents>') != *"</LesHouchesEvents>"* ]]; then
      RUN_STATUS=98
    else
      nevtbegin=$(grep -e '<event>' cmsgrid_final.lhe | wc -l)
      nevtend=$(grep -e '</event>' cmsgrid_final.lhe | wc -l)
      if [[ $nevtbegin -ne $nevtend ]]; then
        RUN_STATUS=97
      fi
    fi
  fi

  echo "- Iteration is done."
  echo "time: $(date +%s)"

  LHE_SEED=$(( LHE_SEED + 1 ))
  LHE_ITER=$(( LHE_ITER + 1 ))
  if [[ ${LHE_ITER} -eq 1000 ]]; then
    break
  fi
done

if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "LHE gridpack step failed with error code ${RUN_STATUS}."
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "LHE GRIDPACK SUCCESSFUL"
fi
