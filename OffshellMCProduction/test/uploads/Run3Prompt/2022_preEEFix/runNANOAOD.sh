#!/bin/bash

NCPUS=1

scrdir="$(dirname $0)"
cd ${scrdir}


scram_arch=el8_amd64_gcc11
cmssw_version=CMSSW_13_0_13
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

if [[ ! -e nanoaod_cfg.py ]]; then
  echo "Creating the NANOAOD cfg"
  cmsDriver.py --python_filename nanoaod_cfg.py \
    --eventcontent NANOAODSIM --datatier NANOAODSIM --filein file:miniaod.root --fileout file:nanoaod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 130X_mcRun3_2022_realistic_v5 --step NANO --era Run3 \
    --no_exec --mc -n -1 || exit $?
fi

cmd="cmsRun -n ${NCPUS} nanoaod_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_nanoaod.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "NANOAOD step failed with error code ${RUN_STATUS}. Output log:"
  cat log_nanoaod.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "NANOAOD SUCCESSFUL"
fi
