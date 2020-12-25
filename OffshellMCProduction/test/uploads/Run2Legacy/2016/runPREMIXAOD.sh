#!/bin/bash

NCPUS=$1

export SCRAM_ARCH=slc6_amd64_gcc530

source /cvmfs/cms.cern.ch/cmsset_default.sh
if [ -r CMSSW_8_0_21/src ]; then
  echo release CMSSW_8_0_21 already exists
else
  scram p CMSSW CMSSW_8_0_21
fi
cd CMSSW_8_0_21/src
eval $(scram runtime -sh)

scram b
cd ../..


if [[ ! -e premixraw_cfg.py ]]; then
  echo "Creating the PREMIX-RAW cfg"
  cmsDriver.py --python_filename premixraw_cfg.py \
    --eventcontent PREMIXRAW --datatier GEN-SIM-RAW --filein file:gensim.root --fileout file:premixraw.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --pileup_input "dbs:/Neutrino_E-10_gun/RunIISpring15PrePremix-PUMoriond17_80X_mcRun2_asymptotic_2016_TrancheIV_v2-v2/GEN-SIM-DIGI-RAW" \
    --conditions 80X_mcRun2_asymptotic_2016_TrancheIV_v6 --step DIGIPREMIX_S2,DATAMIX,L1,DIGI2RAW,HLT:@frozen2016  --datamix PreMix --era Run2_2016 \
    --no_exec --mc -n -1 || exit $?
fi
if [[ ! -e aod_cfg.py ]]; then
  echo "Creating the AOD cfg"
  cmsDriver.py --python_filename aod_cfg.py \
    --eventcontent AODSIM --datatier AODSIM --filein file:premixraw.root --fileout file:aod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 80X_mcRun2_asymptotic_2016_TrancheIV_v6 --step RAW2DIGI,RECO,EI --era Run2_2016 \
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
