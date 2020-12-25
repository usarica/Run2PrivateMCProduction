#!/bin/bash

chkdir=$1

for f in $(find $chkdir -name condor.sub); do
  d=${f//\/condor.sub}
  cd $d
  echo "Processing $d"
  rm -f Logs/prior_record.tar
  for prevjob in $(ls ./ | grep ".log"); do
    prevjob=${prevjob//".log"}
    tar Jcf "prior_record.${prevjob}.tar" Logs/*${prevjob}* "${prevjob}.log" --exclude={*.tar}
    rm "${prevjob}.log"
    rm Logs/*${prevjob}*
  done

  condor_submit condor.sub

  cd - &> /dev/null
done
