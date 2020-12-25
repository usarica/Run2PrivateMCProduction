#!/bin/bash


SUBMITDIR=$1
declare -i NEVTS=$2
declare -i SEED=$3
declare -i NCPUS=$4
CONDORSITE=$5
CONDOROUTDIR=$6


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

  # FIRST TRY GSIFTP
  COPY_SRC="file://${INPUTDIR}/${FILENAME}"
  # IF THE PROTOCOL IS NOT SPECIFIED, DO SOME GUESSWORK
  # Found these from https://fts3.cern.ch:8449/fts3/ftsmon and https://gitlab.cern.ch/SITECONF together, please check.
  if [[ "${OUTPUTSITE}" != *"://"* ]]; then
    COPY_DEST="gsiftp://gftp.${OUTPUTSITE}${OUTPUTDIR}/${RENAMEFILE}"
    if [[ "${OUTPUTSITE}" == *"eoscms.cern.ch"* ]]; then
      COPY_DEST="gsiftp://eoscmsftp.cern.ch${OUTPUTDIR/'/eos/cms'/''}/${RENAMEFILE}"
    elif [[ "${OUTPUTSITE}" == *"ihep.ac.cn"* ]]; then
      # PLEASE CHECK THE PORT ON THIS LINE AND ADJUST OUTPUTDIR IF NEEDED
      COPY_DEST="gsiftp://ccsrm.ihep.ac.cn:2811${OUTPUTDIR}/${RENAMEFILE}"
    fi
  else
    COPY_DEST="${OUTPUTSITE}${OUTPUTDIR}/${RENAMEFILE}"
  fi
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
  # IF GSIFTP OR USER-SPECIFIED SITE PROTOCOL FAILS, FALLBACK TO MORE SITE-SPECIFIC PROTOCOL GUESSWORK
  # (AH, MONKEYS!)
  if [[ $COPY_STATUS -ne 0 ]]; then
    if [[ "${OUTPUTSITE}" == *"t2.ucsd.edu"* ]]; then
      COPY_DEST="davs://redirector.t2.ucsd.edu:1094${OUTPUTDIR}/${RENAMEFILE}"
      COPY_DEST=${COPY_DEST/'/hadoop/cms'/''}
    elif [[ "${OUTPUTSITE}" == *"eoscms.cern.ch"* ]]; then
      COPY_DEST="root://eoscms.cern.ch${OUTPUTDIR}/${RENAMEFILE}"
      COPY_DEST=${COPY_DEST/'/eos/cms'/''}
    elif [[ "${OUTPUTSITE}" == *"iihe.ac.be"* ]]; then
      # See https://t2bwiki.iihe.ac.be/GridStorageAccess
      COPY_DEST="srm://maite.iihe.ac.be:8443${OUTPUTDIR}/${RENAMEFILE}"
      #COPY_DEST=${COPY_DEST/'/pnfs/iihe/cms'/''}
    elif [[ "${OUTPUTSITE}" == *"ihep.ac.cn"* ]]; then
      # PLEASE CHANGE THE TWO LINES BELOW FOR IHEP CN
      # THE FIRST ADJUSTS PROTOCOL, AND PORT IF NEEDED
      # THE SECOND ADJUSTS DESTINATION FILE NAME BUT IS COMMENTED OUT UNTIL YOU CHECK
      COPY_DEST="srm://srm.ihep.ac.cn:8443${OUTPUTDIR}/${RENAMEFILE}"
      #COPY_DEST=${COPY_DEST/'/data/cms'/''}
    elif [[ "${OUTPUTSITE}" == *"m45.ihep.su"* ]]; then
      # PLEASE CHANGE THE TWO LINES BELOW FOR IHEP RU
      # THE FIRST ADJUSTS PROTOCOL, AND PORT IF NEEDED
      # THE SECOND ADJUSTS DESTINATION FILE NAME BUT IS COMMENTED OUT UNTIL YOU CHECK
      COPY_DEST="srm://dp0015.m45.ihep.su:8443${OUTPUTDIR}/${RENAMEFILE}"
      #COPY_DEST=${COPY_DEST/'/pnfs/m45.ihep.su/data/cms'/''}
    fi
    echo "Running alternative endpoint: env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 14400 --verbose --checksum ADLER32 ${COPY_SRC} ${COPY_DEST}"
    while [[ $itry -lt 10 ]]; do
      env -i X509_USER_PROXY=${X509_USER_PROXY} gfal-copy -p -f -t 14400 --verbose --checksum ADLER32 ${COPY_SRC} ${COPY_DEST}
      COPY_STATUS=$?
      if [[ $COPY_STATUS -eq 0 ]]; then
        break
      fi
      (( itry += 1 ))
    done
  fi
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



echo "GLIDEIN_CMSSite: $GLIDEIN_CMSSite"
echo "hostname: $(hostname)"
echo "uname -a: $(uname -a)"
echo "time: $(date +%s)"
echo "args: $@"

setupenv

for f in $(ls ./ | grep -e .tar); do
  tar xzvf $f
  if [[ $? -ne 0 ]]; then
    tar xvf $f
  fi
  rm $f
done

RUNDIR=$(pwd)

# This is a file to keep a list of transferables
touch EXTERNAL_TRANSFER_LIST.LST

chmod 755 runcmsgrid.sh
declare -i LHE_STATUS=1
declare -i LHE_SEED=${SEED}
declare -i LHE_ITER=0
while [[ ${LHE_STATUS} -ne 0 ]]; do
  echo "Grid pack iteration ${LHE_ITER} with seed ${LHE_SEED}"
  echo "time: $(date +%s)"

  ./runcmsgrid.sh ${NEVTS} ${LHE_SEED} ${NCPUS}
  LHE_STATUS=$?

  if [[ ${LHE_STATUS} -eq 0 ]]; then
    if [[ ! -s cmsgrid_final.lhe ]]; then
      LHE_STATUS=99
    elif [[ $(tail -10 cmsgrid_final.lhe | grep -e '</LesHouchesEvents>') != *"</LesHouchesEvents>"* ]]; then
      LHE_STATUS=98
    else
      nevtbegin=$(grep -e '<event>' cmsgrid_final.lhe | wc -l)
      nevtend=$(grep -e '</event>' cmsgrid_final.lhe | wc -l)
      if [[ $nevtbegin -ne $nevtend ]]; then
        LHE_STATUS=97
      fi
    fi
  fi

  echo "- Iteration is done."
  echo "time: $(date +%s)"

  LHE_SEED=$(( LHE_SEED + 1 ))
  LHE_ITER=$(( LHE_ITER + 1 ))
  if [[ ${LHE_ITER} -eq 1000 ]]; then
    break
  fi
done

if [[ ${LHE_STATUS} -ne 0 ]]; then
  echo "LHE generation failed with exit status ${LHE_STATUS}."
  exit 1
else
  echo "LHE GRIDPACK SUCCESSFUL"
fi


declare -i RUN_STATUS=1
echo "time: $(date +%s)"
./runLHEGENSIM.sh ${NCPUS}
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  exit 1
fi

RUN_STATUS=1
echo "time: $(date +%s)"
./runPREMIXAOD.sh ${NCPUS}
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  exit 1
fi

# MINIAODSIM AND NANOAODSIM steps must use ncpus=1
# This is to avoid technicalitiies in TFormula evaluations.
# (AH, ROOT MONKEYS!)
RUN_STATUS=1
echo "time: $(date +%s)"
./runMINIAOD.sh
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  exit 1
fi
echo "time: $(date +%s)"

RUN_STATUS=1
echo "time: $(date +%s)"
./runNANOAOD.sh
RUN_STATUS=$?
if [[ ${RUN_STATUS} -ne 0 ]]; then
  exit 1
fi


echo "All steps are done. Preparing for transfer..."
echo "time: $(date +%s)"


# Rename and tar LHE files and move to LHE folder
# Making an LHE directory not only puts the tarball inside but also helps create the LHE output subdirectory
mkdir LHE
mv cmsgrid_final.lhe cmsgrid_final_${SEED}.lhe
mv cmsgrid_tmp.lhe cmsgrid_final_undecayed_${SEED}.lhe
tar Jcvf output_${SEED}.tar cmsgrid_final_${SEED}.lhe cmsgrid_final_undecayed_${SEED}.lhe
mv output_${SEED}.tar LHE/
echo LHE/output_${SEED}.tar >> EXTERNAL_TRANSFER_LIST.LST

# Make the MINIAODSIM directory and move the file there
mkdir MINIAODSIM
mv miniaod.root MINIAODSIM/output_${SEED}.root
echo MINIAODSIM/output_${SEED}.tar >> EXTERNAL_TRANSFER_LIST.LST

# Make the NANOAODSIM directory and move the file there
mkdir NANOAODSIM
mv nanoaod.root NANOAODSIM/output_${SEED}.root
echo NANOAODSIM/output_${SEED}.tar >> EXTERNAL_TRANSFER_LIST.LST


echo "Files being transfered:"
cat EXTERNAL_TRANSFER_LIST.LST


echo -e "\n--- Begin EXTERNAL TRANSFER ---\n"
while IFS='' read -r line || [[ -n "$line" ]]; do
  OUTFILENAME=${line}
  echo "Copying output file ${OUTFILENAME}"
  copyFromCondorToSite.sh ${RUNDIR} ${OUTFILENAME} ${CONDORSITE} ${CONDOROUTDIR}
  TRANSFER_STATUS=$?
  if [ $TRANSFER_STATUS != 0 ]; then
    echo " - Transfer crashed with exit code ${TRANSFER_STATUS}"
    exit 1
  fi
done < "EXTERNAL_TRANSFER_LIST.LST"
echo -e "\n--- End EXTERNAL TRANSFER ---\n"
