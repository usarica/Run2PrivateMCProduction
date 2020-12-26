#!/bin/bash

if [[ $# -ne 2 ]]; then
  echo "You must specify the check directory as the first argument, and your email as the second."
  exit 1
fi

chkdir=$1
mymail=$2
chkdir=${chkdir%'/'}
curdir=$(pwd)
curdir=${curdir%'/'}
curdir="${curdir}/"
chkdir=${chkdir//${curdir}}
if [[ "${chkdir}" != *"tasks/"* ]]; then
  echo "The check directory must be a subdirectory under 'tasks/'."
  exit 1
fi
echo "CondorWatch is launched for ${chkdir}"
watchlog=watchlog_${chkdir##*/}.txt
thehost=$(hostname)

USER_ID=$(id -u)
homedir=$(readlink -f ~)
proxy_file=${homedir}/x509up_u${USER_ID}
let proxy_valid_threshold=86400 # 1 day

time_offset=$(date +%s)

webdir=""
if [[ -d ${homedir}/www ]]; then
  webdir=${homedir}/www/PrivateMC
  mkdir -p ${webdir}
elif [[ -d ${homedir}/public_html ]]; then
  webdir=${homedir}/public_html/PrivateMC
  mkdir -p ${webdir}
fi
if [[ ! -z ${webdir} ]]; then
  echo "The logs of jobs will be placed daily in ${webdir}."
fi

declare -i nTOTAL=0
declare -i nFAILED=0
declare -i proxytime=0
while [[ 1 ]]; do
  proxytime=$(voms-proxy-info --timeleft --file=${proxy_file})
  if [[ ${proxytime} -lt ${proxy_valid_threshold} ]]; then
    mail -s "[CondorWatch] ($(whoami):${thehost}) WARNING" $mymail <<< "Your VOMS proxy file ${proxy_file} will expire in less than 1 day. Please run ./setup.sh under $(pwd) urgently. Otherwise, this script will nag you every hour."
  fi

  rm -f ${watchlog}
  checkPrivateMCJobs.sh ${chkdir} &> ${watchlog}
  if [[ $? -ne 0 ]]; then
    mail -s "[CondorWatch] ($(whoami):${thehost}) ERROR" $mymail <<< "Command 'checkPrivateMCJobs.sh ${chkdir} &> ${watchlog}' failed with error code $?. The script has aborted. Please check the file ${watchlog} for hints."
    exit 1
  fi
  nTOTAL=$(grep -e "Total jobs checked" ${watchlog} | awk '{print $4}')
  nSUCCESS=$(grep -e "ran successfully" ${watchlog} | wc -l)
  if [[ ${nTOTAL} -eq 0 ]] || [[ ${nSUCCESS} -eq ${nTOTAL} ]]; then
    break
  fi

  # Produce a daily report
  current_time=$(date +%s)
  time_difference=$(( current_time - time_offset ))
  #if [[ ${time_difference} -ge 86400 ]]; then
  if [[ ${time_difference} -ge 0 ]]; then
    time_offset=${current_time}
    if [[ ! -z ${webdir} ]]; then
      rm -rf ${webdir}/*
      cp ${watchlog} ${webdir}/
      mkdir -p ${webdir}/FailedJobs
      for dd in $(grep -e "failed" ${watchlog} | awk '{print $1}'); do
        newlogdir=${dd//${chkdir}}
        mkdir -p ${webdir}/FailedJobs/${newlogdir}
        for ff in $(ls ${dd}/Logs | grep -v .tar); do
          cp $ff ${webdir}/FailedJobs/${newlogdir}/
        done
      done
      chmod -R 777 ${webdir}
      mail -s "[CondorWatch] ($(whoami):${thehost}) INFO" $mymail <<< "The logs of failed jobs are placed in ${webdir} for your daily scrutiny."
    fi
  fi

  for dd in $(grep -e "failed" ${watchlog} | awk '{print $1}'); do
    resubmitPrivateMCJobs.sh ${dd}
    if [[ $? -ne 0 ]]; then
      mail -s "[CondorWatch] ($(whoami):${thehost}) ERROR" $mymail <<< "Command 'resubmitPrivateMCJobs.sh ${dd}' failed with error code $?. The script has aborted."
      exit 1
    fi
  done
  sleep 3600
done

mail -s "[CondorWatch] ($(whoami):${thehost}) FINAL REPORT" $mymail <<< "All jobs under $(readlink -f $chkdir) have completed successfully. Please do not forget to exit your screen if you opened one for this watch."
rm -f ${watchlog}
