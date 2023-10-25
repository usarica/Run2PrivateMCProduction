#!/bin/bash

NCPUS=1

scrdir="$(dirname $0)"
cd ${scrdir}


scram_arch=slc7_amd64_gcc700
cmssw_version=CMSSW_10_6_25
export SCRAM_ARCH=${scram_arch}
source /cvmfs/cms.cern.ch/cmsset_default.sh
if [[ -r ${cmssw_version}/src ]]; then
  echo release ${cmssw_version} already exists
else
  scram p ${cmssw_version}
fi
cd ${cmssw_version}/src
eval $(scram runtime -sh)
scram b
cd ../..

if [[ ! -e miniaod_cfg.py ]]; then
  echo "Creating the MINIAOD cfg"
  cmsDriver.py --python_filename miniaod_cfg.py \
    --eventcontent MINIAODSIM  --datatier MINIAODSIM --filein file:aod.root --fileout file:miniaod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 106X_mcRun2_asymptotic_preVFP_v11 --step PAT --procModifiers run2_miniAOD_UL --geometry DB:Extended --era Run2_2016_HIPM \
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