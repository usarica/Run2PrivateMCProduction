#!/bin/bash

USER_ID=$(id -u)
homedir=$(readlink -f ~)
proxy_file=${homedir}/x509up_u${USER_ID}
let proxy_valid_threshold=86400 # 1 day

if [[ ! -f ${proxy_file} ]] || [[ $(voms-proxy-info --timeleft --file=${proxy_file}) -lt ${proxy_valid_threshold} ]]; then
  # Obtain proxy for 8 days
  voms-proxy-init -q -rfc -voms cms -hours 192 -valid=192:0 -out=${proxy_file}
fi

command -v myschedd &> /dev/null
if [[ $? -eq 0 ]]; then
  if [[ ! -e FIXED_SCHEDD ]] || [[ -z "$(cat FIXED_SCHEDD)" ]]; then
    myschedd bump
    myschedd show | grep -e "Current schedd" | awk '{print $3}' >> FIXED_SCHEDD
  else
    strfixedschedd=$(cat FIXED_SCHEDD)
    echo "Switching to the scheduler ${strfixedschedd} because this is what was used before."
    myschedd set ${strfixedschedd}
  fi
fi
