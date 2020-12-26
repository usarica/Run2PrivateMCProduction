#!/bin/bash

INFILE=$1
users=( usarica sicheng npostiau hanwen mmahdavi lyuan )

first=1
theUser=0
while IFS='' read -r line || [[ -n "$line" ]]; do
  if [[ $first -eq 1 ]]; then
    for user in "${users[@]}"; do
      echo "$line" >> ${INFILE/.csv/_${user}.csv}
    done
    first=0
  else
    echo "$line" >> ${INFILE/.csv/_${users[$theUser]}.csv}
    theUser=$(( theUser + 1 ))
    if [[ $theUser -eq ${#users[@]} ]]; then
      theUser=0
    fi
  fi
done < "$INFILE"
