#!/bin/bash

NCPUS=1

export SCRAM_ARCH=slc6_amd64_gcc700

source /cvmfs/cms.cern.ch/cmsset_default.sh
if [ -r CMSSW_10_2_22/src ] ; then
  echo release CMSSW_10_2_22 already exists
else
  scram p CMSSW CMSSW_10_2_22
fi
cd CMSSW_10_2_22/src
eval $(scram runtime -sh)

scram b
cd ../..


if [[ ! -e nanoaod_cfg.py ]]; then
  echo "Creating the NANOAOD v7 cfg"
  cmsDriver.py --python_filename nanoaod_cfg.py \
    --eventcontent NANOAODSIM --datatier NANOAODSIM --filein file:miniaod.root --fileout file:nanoaod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 102X_mcRun2_asymptotic_v8 --step NANO --era Run2_2016,run2_nanoAOD_94X2016 \
    --no_exec --mc -n -1 || exit $?
fi


cmd="cmsRun -n ${NCPUS} nanoaod_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_nanoaod.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "NANOAOD step failed with error code ${RUN_STATUS}. Output log:"
  cat log_nanoaod.txt
else
  echo "NANOAOD SUCCESSFUL"
fi
