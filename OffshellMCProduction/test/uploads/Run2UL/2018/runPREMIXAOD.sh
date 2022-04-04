#!/bin/bash

NCPUS=$1

scrdir="$(dirname $0)"
cd ${scrdir}


scram_arch=slc7_amd64_gcc700
cmssw_version=CMSSW_10_6_17_patch1
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

if [[ ! -e premixraw_cfg.py ]]; then
  echo "Creating the PREMIX-RAW cfg"
  cmsDriver.py --python_filename premixraw_cfg.py \
    --eventcontent PREMIXRAW --datatier GEN-SIM-DIGI --filein file:gensim.root --fileout file:premixraw.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --pileup_input "dbs:/Neutrino_E-10_gun/RunIISummer20ULPrePremix-UL18_106X_upgrade2018_realistic_v11_L1v1-v2/PREMIX" \
    --conditions 106X_upgrade2018_realistic_v11_L1v1 --step DIGI,DATAMIX,L1,DIGI2RAW --procModifiers premix_stage2 --geometry DB:Extended \
    --datamix PreMix --era Run2_2018 --runUnscheduled \
    --no_exec --mc -n -1 || exit $?
fi

cmd="cmsRun -n ${NCPUS} premixraw_cfg.py"
echo "Running ${cmd}"
# Try the premix step twice because it could sometimes just fail to connect.
${cmd} &> log_premixraw.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "PREMIX-RAW step failed once with error code ${RUN_STATUS}. Output:"
  cat log_premixraw.txt
  echo "Trying a second time after 30 minutes..."
  sleep 1800
  rm -f log_premixraw.txt
  echo "Running ${cmd} one last time..."
  ${cmd} &> log_premixraw.txt
  RUN_STATUS=$?
fi
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "PREMIX-RAW step failed with error code ${RUN_STATUS}. Output:"
  cat log_premixraw.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "PREMIX-RAW SUCCESSFUL"
fi


## Goodness, what a terrible configuration...

# HLT step is separate from AOD in UL
scram_arch=slc7_amd64_gcc700
cmssw_version=CMSSW_10_2_16_UL
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

if [[ ! -e hlt_cfg.py ]]; then
  echo "Creating the HLT cfg"
  cmsDriver.py --python_filename hlt_cfg.py \
    --eventcontent RAWSIM --datatier GEN-SIM-RAW --filein file:premixraw.root --fileout file:hlt.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 102X_upgrade2018_realistic_v15 --customise_commands 'process.source.bypassVersionCheck = cms.untracked.bool(True)' --step HLT:2018v32 --geometry DB:Extended --era Run2_2018 \
    --no_exec --mc -n -1 || exit $?
fi

cmd="cmsRun -n 1 hlt_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_hlt.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "HLT step failed with error code ${RUN_STATUS}. Output:"
  cat log_hlt.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "HLT SUCCESSFUL"
fi


# AOD step
scram_arch=slc7_amd64_gcc700
cmssw_version=CMSSW_10_6_17_patch1
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

if [[ ! -e aod_cfg.py ]]; then
  echo "Creating the AOD cfg"
  cmsDriver.py --python_filename aod_cfg.py \
    --eventcontent AODSIM --datatier AODSIM --filein file:hlt.root --fileout file:aod.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 106X_upgrade2018_realistic_v11_L1v1 --step RAW2DIGI,L1Reco,RECO,RECOSIM,EI --geometry DB:Extended --era Run2_2018 \
    --runUnscheduled --no_exec --mc -n -1 || exit $?
fi

cmd="cmsRun -n 1 aod_cfg.py"
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
  rm -f hlt.root # Remove the HLT file to retain disk space
fi
