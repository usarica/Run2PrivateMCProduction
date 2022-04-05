#!/bin/bash

NCPUS=1

scrdir="$(dirname $0)"
cd ${scrdir}

export SCRAM_ARCH=slc6_amd64_gcc630

source /cvmfs/cms.cern.ch/cmsset_default.sh
if [ -r CMSSW_9_4_7/src ] ; then
  echo release CMSSW_9_4_7 already exists
else
  scram p CMSSW CMSSW_9_4_7
fi
cd CMSSW_9_4_7/src
eval $(scram runtime -sh)

scram b
cd ../..


if [[ ! -e miniaod_cfg.py ]]; then
  echo "Creating the MINIAOD cfg"
  cmsDriver.py --python_filename miniaod_cfg.py \
    --eventcontent MINIAODSIM  --datatier MINIAODSIM --filein file:aod.root --fileout file:miniaod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 94X_mc2017_realistic_v14 --step PAT --scenario pp --era Run2_2017,run2_miniAOD_94XFall17 \
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

cmd="cmsRun -n 1 xsec_cfg.py"
echo "Running ${cmd}"
${cmd} &> xsec.txt # The log is the output in this case.
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "XSEC step failed with error code ${RUN_STATUS}. Output log:"
  cat xsec.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "XSEC SUCCESSFUL"
fi
