#!/bin/bash

tag=$1

(

cd ${CMSSW_BASE}/src/Run2PrivateMCProduction/OffshellMCProduction/test

# Kill all idle or held jobs
condor_rm $(whoami) -constraint "JobStatus==1 || JobStatus==5"
# Also remove from local pool immediately
condor_rm -forcex $(whoami) -constraint "JobStatus==1 || JobStatus==5 || JobStatus=3"

echo "Sleeping for 2 minutes to ensure all jobs are removed locally."
sleep 120

cd ..
git pull
scram b -j
cd -

# Remake the tar files
for year in 2016 2017 2018; do
  rm -f tasks/${tag}/runscripts_${year}.tar
  createPrivateMCRunScriptsTarball.sh Run2Legacy ${year} $(pwd)/tasks/${tag}/
done

# Run the check script
rm -f watchlog_${tag}.txt
checkPrivateMCJobs.sh tasks/${tag} >> watchlog_${tag}.txt
# Resubmit all jobs which got aborted (should be marked as failed)
declare -i njobs=$(grep -e failed watchlog_${tag}.txt | wc -l)

echo "${njobs} jobs will be resubmitted. Waiting for 60 seconds in case you find this is a mistake and want to kill this script."
sleep 60

for dd in $(grep -e failed watchlog_${tag}.txt | awk '{print $1}' | sort | uniq); do
  resubmitPrivateMCJobs.sh $dd
done
rm -f watchlog_${tag}.txt

)
