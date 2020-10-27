#!/bin/bash


CMSSWVERSION=$1
SCRAMARCH=$2
SUBMITDIR=$3
declare -i NEVTS=$4
declare -i SEED=$5
CONDORSITE=$6
CONDOROUTDIR=$7


function setupenv {
    if [ -r "$OSGVO_CMSSW_Path"/cmsset_default.sh ]; then
        echo "sourcing environment: source $OSGVO_CMSSW_Path/cmsset_default.sh"
        source "$OSGVO_CMSSW_Path"/cmsset_default.sh
    elif [ -r "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]; then
        echo "sourcing environment: source $OSG_APP/cmssoft/cms/cmsset_default.sh"
        source "$OSG_APP"/cmssoft/cms/cmsset_default.sh
    elif [ -r /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
        echo "sourcing environment: source /cvmfs/cms.cern.ch/cmsset_default.sh"
        source /cvmfs/cms.cern.ch/cmsset_default.sh
    else
        echo "ERROR! Couldn't find $OSGVO_CMSSW_Path/cmsset_default.sh or /cvmfs/cms.cern.ch/cmsset_default.sh or $OSG_APP/cmssoft/cms/cmsset_default.sh"
        exit 1
    fi
}
function copyFromCondorToSite {
INPUTDIR=$1
FILENAME=$2
OUTPUTSITE=$3 # e.g. 't2.ucsd.edu'
OUTPUTDIR=$4 # Must be absolute path
RENAMEFILE=$FILENAME
if [[ "$5" != "" ]];then
  RENAMEFILE=$5
fi


echo "Copy from Condor is called with"
echo "INPUTDIR: ${INPUTDIR}"
echo "FILENAME: ${FILENAME}"
echo "OUTPUTSITE: ${OUTPUTSITE}"
echo "OUTPUTDIR: ${OUTPUTDIR}"
echo "RENAMEFILE: ${RENAMEFILE}"


if [[ "$INPUTDIR" == "" ]];then #Input directory is empty, so assign pwd
  INPUTDIR=$(pwd)
elif [[ "$INPUTDIR" != "/"* ]];then # Input directory is a relative path
  INPUTDIR=$(pwd)/${INPUTDIR}
fi

if [[ "$OUTPUTDIR" != "/"* ]];then # Output directory must be an absolute path!
  echo "Output directory must be an absolute path! Cannot transfer the file..."
  exit 1
fi


if [[ ! -z ${FILENAME} ]];then
  echo -e "\n--- begin copying output ---\n"

  echo "Sending output file ${FILENAME}"

  if [[ ! -e ${INPUTDIR}/${FILENAME} ]]; then
    echo "ERROR! Output ${FILENAME} doesn't exist"
    exit 1
  fi

  echo "Time before copy: $(date +%s)"

  COPY_SRC="file://${INPUTDIR}/${FILENAME}"
  COPY_DEST="gsiftp://gftp.${OUTPUTSITE}${OUTPUTDIR}/${RENAMEFILE}"
  echo "Running: env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 14400 --verbose --checksum ADLER32 ${COPY_SRC} ${COPY_DEST}"
  declare -i itry=0
  declare -i COPY_STATUS=-1
  declare -i REMOVE_STATUS=-1
  while [[ $itry -lt 5 ]]; do
    env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 14400 --verbose --checksum ADLER32 ${COPY_SRC} ${COPY_DEST}
    COPY_STATUS=$?
    if [[ $COPY_STATUS -eq 0 ]]; then
      break
    fi
    (( itry += 1 ))
  done
  if [[ $COPY_STATUS -ne 0 ]]; then
    echo "Removing output file because gfal-copy crashed with code $COPY_STATUS"
    env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-rm -t 14400 --verbose ${COPY_DEST}
    REMOVE_STATUS=$?
    if [[ $REMOVE_STATUS -ne 0 ]]; then
        echo "gfal-copy crashed and then the gfal-rm also crashed with code $REMOVE_STATUS"
        echo "You probably have a corrupt file sitting on ${OUTPUTDIR} now."
        exit $REMOVE_STATUS
    fi
    exit $COPY_STATUS
  else
    echo "Time after copy: $(date +%s)"
    echo "Copied successfully!"
  fi

  echo -e "\n--- end copying output ---\n"
else
  echo "File name is not specified!"
  exit 1
fi
}



echo "CMSSWVERSION: $CMSSWVERSION"
echo "SCRAMARCH: $SCRAMARCH"

echo "GLIDEIN_CMSSite: $GLIDEIN_CMSSite"
echo "hostname: $(hostname)"
echo "uname -a: $(uname -a)"
echo "time: $(date +%s)"
echo "args: $@"

setupenv

for f in $(ls ./ | grep -e .tar); do
  tar xzvf $f
  rm $f
done

RUNDIR=$(pwd)

chmod 755 runcmsgrid.sh
./runcmsgrid.sh ${NEVTS} ${SEED} 1

RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  echo "Event generation failed with exit status ${RUN_STATUS}."
  exit 1
else
  echo "Event generation is successful."
fi

mv cmsgrid_final.lhe cmsgrid_final_${SEED}.lhe

copyFromCondorToSite ${RUNDIR} cmsgrid_final_${SEED}.lhe ${CONDORSITE} ${CONDOROUTDIR}

TRANSFER_STATUS=$?
if [[ ${TRANSFER_STATUS} -ne 0 ]]; then
  echo "File transfer failed with exit status ${TRANSFER_STATUS}."
  exit 1
fi
