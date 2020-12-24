#!/bin/bash

NCPUS=1

export SCRAM_ARCH=slc6_amd64_gcc700

source /cvmfs/cms.cern.ch/cmsset_default.sh
if [ -r CMSSW_10_2_5/src ] ; then
  echo release CMSSW_10_2_5 already exists
else
  scram p CMSSW CMSSW_10_2_5
fi
cd CMSSW_10_2_5/src
eval $(scram runtime -sh)

scram b
cd ../..


if [[ ! -e miniaod_cfg.py ]]; then
  echo "Creating the MINIAOD cfg"
  cmsDriver.py --python_filename miniaod_cfg.py \
    --eventcontent MINIAODSIM  --datatier MINIAODSIM --filein file:aod.root --fileout file:miniaod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 102X_upgrade2018_realistic_v15 --step PAT --geometry DB:Extended --era Run2_2018 \
    --runUnscheduled --no_exec --mc -n -1 || exit $?
fi

cmd="cmsRun -n ${NCPUS} miniaod_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_miniaod.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "MINIAOD step failed with error code ${RUN_STATUS}. Output log:"
  cat log_miniaod.txt
else
  echo "MINIAOD SUCCESSFUL"
fi
