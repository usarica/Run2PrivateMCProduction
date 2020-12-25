#!/bin/bash

USER_ID=$(id -u)
proxy_file="${CMSSW_BASE}/src/Run2PrivateMCProduction/OffshellMCProduction/test/x509up_u${USER_ID}"
let proxy_valid_threshold=86400 # 1 day

if [[ ! -f ${proxy_file} ]] || [[ $(voms-proxy-info --timeleft --file=${proxy_file}) -lt ${proxy_valid_threshold} ]]; then
  voms-proxy-init -q -rfc -voms cms -hours 192 -valid=192:0 -out=${proxy_file}
fi

