#!/bin/bash

NCPUS=1

scrdir="$(dirname $0)"
cd ${scrdir}

export SCRAM_ARCH=slc6_amd64_gcc630

source /cvmfs/cms.cern.ch/cmsset_default.sh
if [ -r CMSSW_9_4_9/src ] ; then
  echo release CMSSW_9_4_9 already exists
else
  scram p CMSSW CMSSW_9_4_9
fi
cd CMSSW_9_4_9/src
eval $(scram runtime -sh)

scram b
cd ../..


if [[ ! -e miniaod_cfg.py ]]; then
  echo "Creating the MINIAOD cfg"
  cmsDriver.py --python_filename miniaod_cfg.py \
    --eventcontent MINIAODSIM  --datatier MINIAODSIM --filein file:aod.root --fileout file:miniaod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 94X_mcRun2_asymptotic_v3 --step PAT --era Run2_2016,run2_miniAOD_80XLegacy \
    --runUnscheduled --no_exec --mc -n -1 || exit $?
fi


cmd="cmsRun -n ${NCPUS} miniaod_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_miniaod.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "MINIAOD step failed with error code ${RUN_STATUS}. Output log:"
  cat log_miniaod.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "MINIAOD SUCCESSFUL"
fi
