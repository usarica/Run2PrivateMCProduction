#!/bin/sh


campaign=$1
year=$2
dest=$3

TARFILE="runscripts_${year}.tar"

HERE=$(pwd)

cd $(mktemp -d)

cp $CMSSW_BASE/src/Run2PrivateMCProduction/OffshellMCProduction/test/uploads/${campaign}/${year}/* ./

tar Jcvf ${TARFILE} *

mv $TARFILE ${dest}

cd ${HERE}

