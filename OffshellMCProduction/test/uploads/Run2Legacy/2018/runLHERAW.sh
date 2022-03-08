#!/bin/bash

NEVTS=$1
SEED=$2
NCPUS=$3

scrdir="$(dirname $0)"
cd ${scrdir}

declare -i RUN_STATUS=1
declare -i LHE_SEED=${SEED}
declare -i LHE_ITER=0
while [[ ${RUN_STATUS} -ne 0 ]]; do
  echo "Grid pack iteration ${LHE_ITER} with seed ${LHE_SEED}"
  echo "time: $(date +%s)"

  rm -f log_rawlhe.txt
  ./runcmsgrid.sh ${NEVTS} ${LHE_SEED} ${NCPUS} &> log_rawlhe.txt
  RUN_STATUS=$?

  if [[ ${RUN_STATUS} -eq 0 ]]; then
    if [[ ! -s cmsgrid_final.lhe ]]; then
      RUN_STATUS=99
    elif [[ $(tail -10 cmsgrid_final.lhe | grep -e '</LesHouchesEvents>') != *"</LesHouchesEvents>"* ]]; then
      RUN_STATUS=98
    else
      nevtbegin=$(grep -e '<event>' cmsgrid_final.lhe | wc -l)
      nevtend=$(grep -e '</event>' cmsgrid_final.lhe | wc -l)
      if [[ ${nevtbegin} -ne ${NEVTS} ]] || [[ ${nevtend} -ne ${NEVTS} ]]; then
        RUN_STATUS=97
      fi
    fi
  fi

  echo "- RUN_STATUS: ${RUN_STATUS}"
  echo "- Iteration is done."
  echo "time: $(date +%s)"

  LHE_SEED=$(( LHE_SEED + 1 ))
  LHE_ITER=$(( LHE_ITER + 1 ))
  if [[ ${LHE_ITER} -eq 1000 ]]; then
    break
  fi
done

if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "LHE gridpack step failed with error code ${RUN_STATUS}. Output:"
  cat log_rawlhe.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "LHE GRIDPACK SUCCESSFUL"
fi
