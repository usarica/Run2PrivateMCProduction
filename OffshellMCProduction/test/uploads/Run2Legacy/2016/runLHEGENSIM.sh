#!/bin/bash

NCPUS=$1

export SCRAM_ARCH=slc6_amd64_gcc481

source /cvmfs/cms.cern.ch/cmsset_default.sh

if [ -r CMSSW_7_1_22/src ] ; then
  echo release CMSSW_7_1_22 already exists
else
  scram p CMSSW CMSSW_7_1_22
fi
cd CMSSW_7_1_22/src
eval $(scram runtime -sh)

mkdir -p Configuration/GenProduction/python
cp ../../fragment.py Configuration/GenProduction/python/gensim-fragment.py
scram b
cd ../..


if [[ ! -e lhe_cfg.py ]]; then
  echo "Creating the LHE cfg"
  cmsDriver.py  \
    --python_filename lhe_cfg.py --eventcontent LHE --datatier LHE  --step NONE --filein file:cmsgrid_final.lhe --fileout file:pLHE.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring  --conditions MCRUN2_71_V1::All --no_exec --mc -n -1 || exit $?
fi
# Always remake the gensim configuration because the Pythia fragment could be different
rm -f gensim_cfg.py
echo "Creating the GEN-SIM cfg"
cmsDriver.py Configuration/GenProduction/python/gensim-fragment.py \
  --python_filename gensim_cfg.py --eventcontent RAWSIM --datatier GEN-SIM --step GEN,SIM --filein file:pLHE.root --fileout file:gensim.root --mc \
  --customise SLHCUpgradeSimulations/Configuration/postLS1Customs.customisePostLS1,Configuration/DataProcessing/Utils.addMonitoring \
  --conditions MCRUN2_71_V1::All --beamspot Realistic50ns13TeVCollision --magField 38T_PostLS1 \
  --no_exec -n -1 || exit $?


cmd="cmsRun -n ${NCPUS} lhe_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_plhe.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "pLHE step failed with error code ${RUN_STATUS}. Output:"
  cat log_plhe.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "pLHE SUCCESSFUL"
fi


cmd="cmsRun -n ${NCPUS} gensim_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_gensim.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "GEN-SIM step failed with error code ${RUN_STATUS}. Output:"
  cat log_gensim.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "GEN-SIM SUCCESSFUL"
fi

