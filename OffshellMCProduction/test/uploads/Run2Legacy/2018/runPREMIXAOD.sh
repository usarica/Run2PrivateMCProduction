#!/bin/bash

NCPUS=$1

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


if [[ ! -e premixraw_cfg.py ]]; then
  echo "Creating the PREMIX-RAW cfg"
  cmsDriver.py --python_filename premixraw_cfg.py \
    --eventcontent PREMIXRAW --datatier GEN-SIM-RAW --filein file:gensim.root --fileout file:premixraw.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --pileup_input "dbs:/Neutrino_E-10_gun/RunIISummer17PrePremix-PUAutumn18_102X_upgrade2018_realistic_v15-v1/GEN-SIM-DIGI-RAW" \
    --conditions 102X_upgrade2018_realistic_v15 --step DIGI,DATAMIX,L1,DIGI2RAW,HLT:@relval2018 --procModifiers premix_stage2 --geometry DB:Extended --datamix PreMix --era Run2_2018 \
    --no_exec --mc -n -1 || exit $?
fi
if [[ ! -e aod_cfg.py ]]; then
  echo "Creating the AOD cfg"
  cmsDriver.py --python_filename aod_cfg.py \
    --eventcontent AODSIM --datatier AODSIM --filein file:premixraw.root --fileout file:aod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 102X_upgrade2018_realistic_v15 --step RAW2DIGI,L1Reco,RECO,RECOSIM,EI --procModifiers premix_stage2 --era Run2_2018 \
    --runUnscheduled --no_exec --mc -n -1 || exit $?
fi


cmd="cmsRun -n ${NCPUS} premixraw_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_premixraw.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "PREMIX-RAW step failed with error code ${RUN_STATUS}. Output:"
  cat log_premixraw.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "PREMIX-RAW SUCCESSFUL"
fi

cmd="cmsRun -n ${NCPUS} aod_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_aod.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "AOD step failed with error code ${RUN_STATUS}. Output:"
  cat log_aod.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "AOD SUCCESSFUL"
fi
