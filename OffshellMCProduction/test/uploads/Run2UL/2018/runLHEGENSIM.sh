#!/bin/bash

NCPUS=$1

scrdir="$(dirname $0)"
cd ${scrdir}


scram_arch=slc7_amd64_gcc700
cmssw_version=CMSSW_10_6_22
export SCRAM_ARCH=${scram_arch}
source /cvmfs/cms.cern.ch/cmsset_default.sh
if [[ -r ${cmssw_version}/src ]]; then
  echo release ${cmssw_version} already exists
else
  scram p ${cmssw_version}
fi
cd ${cmssw_version}/src
eval $(scram runtime -sh)
mkdir -p Configuration/GenProduction/python
cp ../../fragment.py Configuration/GenProduction/python/gen-fragment.py
scram b
cd ../..

if [[ ! -e lhe_cfg.py ]]; then
  echo "Creating the LHE cfg"
  cmsDriver.py  \
    --python_filename lhe_cfg.py --eventcontent LHE --datatier LHE  --step NONE --filein file:cmsgrid_final.lhe --fileout file:pLHE.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 106X_upgrade2018_realistic_v4 --beamspot Realistic25ns13TeVEarly2018Collision --geometry DB:Extended --era Run2_2018 \
    --no_exec --mc -n -1 || exit $?
fi
# Always remake gen_cfg.py because the Pythia fragment could be different
rm -f gen_cfg.py
echo "Creating the GEN cfg"
cmsDriver.py Configuration/GenProduction/python/gen-fragment.py \
  --python_filename gen_cfg.py --eventcontent RAWSIM --datatier GEN-SIM --step GEN --filein file:pLHE.root --fileout file:gen.root --mc \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --conditions 106X_upgrade2018_realistic_v4 --beamspot Realistic25ns13TeVEarly2018Collision --geometry DB:Extended --era Run2_2018 \
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

cmd="cmsRun -n ${NCPUS} gen_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_gen.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "GEN step failed with error code ${RUN_STATUS}. Output:"
  cat log_gen.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "GEN SUCCESSFUL"
fi


# Run the SIM step separately
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

if [[ ! -e sim_cfg.py ]]; then
  echo "Creating the SIM cfg"
  cmsDriver.py \
    --python_filename sim_cfg.py --eventcontent RAWSIM --datatier GEN-SIM --step SIM --filein file:gen.root --fileout file:gensim.root \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --conditions 106X_upgrade2018_realistic_v11_L1v1 --beamspot Realistic25ns13TeVEarly2018Collision --geometry DB:Extended --era Run2_2018 --runUnscheduled \
    --no_exec --mc -n -1 || exit $?
fi

cmd="cmsRun -n ${NCPUS} sim_cfg.py"
echo "Running ${cmd}"
${cmd} &> log_sim.txt
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "SIM step failed with error code ${RUN_STATUS}. Output:"
  cat log_sim.txt
  echo ${RUN_STATUS} >> ERROR
  exit ${RUN_STATUS}
else
  echo "SIM SUCCESSFUL"
  rm -f gen.root # Remove the GEN-only file to retain disk space
fi
