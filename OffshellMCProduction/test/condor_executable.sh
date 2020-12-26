#!/bin/bash


SUBMITDIR=$1
declare -i NEVTS=$2
declare -i SEED=$3
declare -i NCPUS=$4
CONDORSITE=$5
CONDOROUTDIR=$6


function getcmsenvscript {
    if [ -r "$OSGVO_CMSSW_Path"/cmsset_default.sh ]; then
        echo "$OSGVO_CMSSW_Path"/cmsset_default.sh
    elif [ -r "$OSG_APP"/cmssoft/cms/cmsset_default.sh ]; then
        echo "$OSG_APP/cmssoft/cms/cmsset_default.sh"
    elif [ -r /cvmfs/cms.cern.ch/cmsset_default.sh ]; then
        echo "/cvmfs/cms.cern.ch/cmsset_default.sh"
    fi
}
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


if [[ "$INPUTDIR" == "" ]];then # Input directory is empty, so assign pwd
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
  # We check on the 'Running: env -i' part of this line, so KEEP IT UNIQUE.
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



echo "hostname: $(hostname)"
echo "uname -a: $(uname -a)"
echo "time: $(date +%s)"
echo "args: $@"
command -v printenv &> /dev/null
if [[ $? -eq 0 ]]; then
  echo "Printing all environment variables defined in the current job:"
  printenv
fi

setupenv
declare -i HAS_CMSSW=$?
if [[ ${HAS_CMSSW} -ne 0 ]]; then
  echo "CMSSW environment script does not exist."
  exit 1
fi

cmsenvdir=$( getcmsenvscript )
echo "CMSSW environment script: ${cmsenvdir}"
cvmfsdirsplit=( ${cmsenvdir//'/'/' '} )
cvmfshead="/${cvmfsdirsplit[0]}"

CURRENTDIR=$(pwd)
RUNDIR=rundir
mkdir -p ${RUNDIR}
for f in $(ls ./ | grep -e .tar.gz); do
  mv $f ${RUNDIR}/
  cd ${RUNDIR} &> /dev/null
  tar xzvf $f
  rm $f
  cd -
done
for f in $(ls ./ | grep -e .tar); do
  mv $f ${RUNDIR}/
  cd ${RUNDIR} &> /dev/null
  tar xvf $f
  rm $f
  cd -
done
# Also move the Pythia fragment
if [[ -f fragment.py ]]; then
  mv fragment.py ${RUNDIR}/
fi

if [[ "${cmsenvdir}" != "/cvmfs/cms.cern.ch/cmsset_default.sh" ]]; then
  echo "Replacing /cvmfs/cms.cern.ch/cmsset_default.sh in the run scripts with ${cmsenvdir}"
  sed -i "s|/cvmfs/cms.cern.ch/cmsset_default.sh|${cmsenvdir}|g" ${RUNDIR}/run*.sh
fi

echo "Current directory: ${CURRENTDIR}"
ls -la

echo "Run directory: ${CURRENTDIR}/${RUNDIR}"
ls -la ${RUNDIR}


# We need to require SL6 for Run 2 legacy reco. to work.
# If the machine uses SL7, we have to use singularity.
# This is often the case in submissions from lxplus.
MACHINESPECS="$(uname -a)"
echo "Machine specifics: ${MACHINESPECS}"
declare -i FOUND_EL6=0
if [[ "${MACHINESPECS}" == *"el6"* ]]; then
  FOUND_EL6=1
fi

declare -i USE_NATIVE_CALLS=0
if [[ ${FOUND_EL6} -eq 1 ]] && [[ ${HAS_CMSSW} -eq 0 ]]; then
  USE_NATIVE_CALLS=1
  echo "Machine has both CMS environment scripts and runs on SL6 OS. Native calls will be used."
else
  echo "Machine does not have the proper setup to run native calls. Singularity with an SL6 docker container will be used."
fi

# This is a file to keep a list of transferables
touch EXTERNAL_TRANSFER_LIST.LST

# This is for singularity cache to be stored
SINGULARITYARGS="-B ${CURRENTDIR}/${RUNDIR} -B ${cvmfshead} -B /etc/grid-security"
SINGULARITYCONTAINER="docker://cmssw/slc6:latest"

if [[ $USE_NATIVE_CALLS -eq 0 ]]; then
  command -v singularity &> /dev/null
  if [[ $? -ne 0 ]]; then
    echo "ERROR: Singularity was requested, but it is not present."
    exit 1
  fi

  # Singularity cache directory might not be set up.
  if [[ -z ${SINGULARITY_CACHEDIR+x} ]]; then
    if [[ ! -z ${TMPDIR+x} ]]; then
      export SINGULARITY_CACHEDIR="${TMPDIR}/singularity"
    else
      export SINGULARITY_CACHEDIR="/tmp/$(whoami)/singularity"
    fi
  fi

  # Some singularity implementations have older versions, which do not have the no-home option.
  STRRUNNOHOME="$(singularity help exec | grep -e no-home)"
  if [[ ! -z "${STRRUNNOHOME}" ]]; then
    SINGULARITYARGS="${SINGULARITYARGS} --no-home"
  fi

  # Try to execute a very simple command to see if singularity runs correctly
  testcmd="singularity exec ${SINGULARITYARGS} ${SINGULARITYCONTAINER} ls -la"
  echo "Executing singularity test command: ${testcmd}"
  ${testcmd}
  if [[ $? -ne 0 ]]; then
    echo "Test command failed. The machine does not seem to have the correct setup."
    exit 1
  fi
fi

chmod 755 ${RUNDIR}/*

echo "time: $(date +%s)"
if [[ $USE_NATIVE_CALLS -eq 1 ]]; then
  ${RUNDIR}/runLHERAW.sh ${NEVTS} ${SEED} ${NCPUS}
else
  singularity exec ${SINGULARITYARGS} ${SINGULARITYCONTAINER} ${RUNDIR}/runLHERAW.sh ${NEVTS} ${SEED} ${NCPUS} || touch ${RUNDIR}/ERROR
fi
if [[ -e ${RUNDIR}/ERROR ]]; then
  exit 1
fi

echo "time: $(date +%s)"
if [[ $USE_NATIVE_CALLS -eq 1 ]]; then
  ${RUNDIR}/runLHEGENSIM.sh ${NCPUS}
else
  singularity exec ${SINGULARITYARGS} ${SINGULARITYCONTAINER} ${RUNDIR}/runLHEGENSIM.sh ${NCPUS} || touch ${RUNDIR}/ERROR
fi
if [[ -e ${RUNDIR}/ERROR ]]; then
  exit 1
fi

echo "time: $(date +%s)"
if [[ $USE_NATIVE_CALLS -eq 1 ]]; then
  ${RUNDIR}/runPREMIXAOD.sh ${NCPUS}
else
  singularity exec ${SINGULARITYARGS} ${SINGULARITYCONTAINER} ${RUNDIR}/runPREMIXAOD.sh ${NCPUS} || touch ${RUNDIR}/ERROR
fi
if [[ -e ${RUNDIR}/ERROR ]]; then
  exit 1
fi

# MINIAODSIM AND NANOAODSIM steps must use ncpus=1
# This is to avoid technicalitiies in TFormula evaluations.
echo "time: $(date +%s)"
if [[ $USE_NATIVE_CALLS -eq 1 ]]; then
  ${RUNDIR}/runMINIAOD.sh
else
  singularity exec ${SINGULARITYARGS} ${SINGULARITYCONTAINER} ${RUNDIR}/runMINIAOD.sh || touch ${RUNDIR}/ERROR
fi
if [[ -e ${RUNDIR}/ERROR ]]; then
  exit 1
fi

echo "time: $(date +%s)"
if [[ $USE_NATIVE_CALLS -eq 1 ]]; then
  ${RUNDIR}/runNANOAOD.sh
else
  singularity exec ${SINGULARITYARGS} ${SINGULARITYCONTAINER} ${RUNDIR}/runNANOAOD.sh || touch ${RUNDIR}/ERROR
fi
if [[ -e ${RUNDIR}/ERROR ]]; then
  exit 1
fi

# Move everything we need back to this directory
mv ${RUNDIR}/*.lhe ./
mv ${RUNDIR}/*.root ./

echo "All steps are done. Preparing for transfer..."
echo "time: $(date +%s)"


# Rename and tar LHE files and move to LHE folder
# Making an LHE directory not only puts the tarball inside but also helps create the LHE output subdirectory
mkdir LHE
mv cmsgrid_final.lhe cmsgrid_final_${SEED}.lhe
mv cmsgrid_tmp.lhe cmsgrid_final_undecayed_${SEED}.lhe
chmod 775 cmsgrid_final*.lhe
tar Jcvf output_${SEED}.tar cmsgrid_final_${SEED}.lhe cmsgrid_final_undecayed_${SEED}.lhe
mv output_${SEED}.tar LHE/
chmod -R 775 LHE
echo LHE/output_${SEED}.tar >> EXTERNAL_TRANSFER_LIST.LST

# Make the MINIAODSIM directory and move the file there
mkdir MINIAODSIM
mv miniaod.root MINIAODSIM/output_${SEED}.root
chmod -R 775 MINIAODSIM
echo MINIAODSIM/output_${SEED}.root >> EXTERNAL_TRANSFER_LIST.LST

# Make the NANOAODSIM directory and move the file there
mkdir NANOAODSIM
mv nanoaod.root NANOAODSIM/output_${SEED}.root
chmod -R 775 NANOAODSIM
echo NANOAODSIM/output_${SEED}.root >> EXTERNAL_TRANSFER_LIST.LST


echo "Files being transfered:"
cat EXTERNAL_TRANSFER_LIST.LST


echo -e "\n--- Begin EXTERNAL TRANSFER ---\n"
while IFS='' read -r line || [[ -n "$line" ]]; do
  OUTFILENAME=${line}
  echo "Copying output file ${OUTFILENAME}"
  copyFromCondorToSite ${CURRENTDIR} ${OUTFILENAME} ${CONDORSITE} ${CONDOROUTDIR}
  TRANSFER_STATUS=$?
  if [ $TRANSFER_STATUS != 0 ]; then
    echo " - Transfer crashed with exit code ${TRANSFER_STATUS}"
    exit 1
  fi
done < "EXTERNAL_TRANSFER_LIST.LST"
echo -e "\n--- End EXTERNAL TRANSFER ---\n"
