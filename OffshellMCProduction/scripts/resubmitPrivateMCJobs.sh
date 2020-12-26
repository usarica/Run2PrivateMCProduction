#!/bin/bash

chkdir=$1

for f in $(find $chkdir -name condor.sub); do
  d=${f//\/condor.sub}
  cd $d
  echo "Processing $d"
  rm -f Logs/prior_record.tar
  for prevjob in $(ls ./ | grep ".log"); do
    prevjob=${prevjob//".log"}
    strstdlogs=""
    for stdlog in $(ls Logs | grep -e ${prevjob}); do
      strstdlogs="${strstdlogs} Logs/${stdlog}"
    done
    tar Jcf "prior_record.${prevjob}.tar" ${strstdlogs} "${prevjob}.log" --exclude={*.tar}
    rm "${prevjob}.log"
    for stdlog in $(echo ${strstdlogs}); do
      rm ${stdlog}
    done
  done

  condor_submit condor.sub

  cd - &> /dev/null
done
