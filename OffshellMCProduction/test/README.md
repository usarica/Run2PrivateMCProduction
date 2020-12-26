# Instructions for using this repository

1. Before you do anything else, make sure you have valid grid certificates on the machines you are going to run (i.e. lxplus, ucsd). You may refer to [this page](https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookStartingGrid).

2. Take a note of the machine you logged in (important!) and get into a screen via

(lxplus)

```
AKLOG=/usr/bin/aklog krenew -b -t -- screen -D -m # This is to be used on lxplus to get persistent screens. Otherwise, use 'screen'
screen -x
```

(UCSD)

```
screen
```

and then set up your CMSSW area:
```
cd [YOUR WORK AREA] # Inside the screen
export SCRAM_ARCH=slc7_amd64_gcc700 # can also use slc6_amd64_gcc700, depends on the OS of your machine
source /cvmfs/cms.cern.ch/cmsset_default.sh
scram p CMSSW CMSSW_10_2_22
cd CMSSW_10_2_22/src
eval $(scram runtime -sh)
```

3. Checkout the repository and compile while inside CMSSW_10_2_22/src:

```
git clone git@github.com:usarica/Run2PrivateMCProduction.git
cd Run2PrivateMCProduction/OffshellMCProduction
scram b
```

4. Set up the submission area inside the directory test/:

```
cd test
./setup.sh
```

You have to run this step every time you log in (to the same machine) or open up a new screen so that you never lose track of your jobs. It will also ensure your grid proxy is up-to-date.

This step must succeed without errors. If not, please email the author of this repository immediately for assistance.

The step will create a FIXED_SCHEDD in lxplus. Please do not tamper with it (or at least note down the scheduler somewhere else).
Otherwise you will have to search over the bigbird machines to find where your jobs are.

5. Submit a test job while you are inside the screen. This job must finish within about an hour. The command below will send you an email when the test batch is successful:

```
python submit_jobs.py --tag cern_test_$(whoami) --gridpack_dir=/afs/cern.ch/work/u/usarica/public/Offshell_2l2nu (/home/users/usarica/work/GenStudies/Offshell2020_Gridpacks on UCSD) \
  --condor_site=t2.ucsd.edu (or iihe.ac.be) --condor_outdir=/hadoop/cms/store/user/$(whoami)/Offshell_2L2Nu/PrivateMC (or /pnfs/iihe/cms/store/user/$(whoami)/Offshell_2L2Nu/PrivateMC) \
  --testrun test.csv --watch_email="[YOUR EMAIL]"
```

Please select the appropriate sites and paths based on the origin and target sites (you have to have write permissions on the target sites). The test is basically going to check if you have this permission.
If the test fails (watch keeps resubmitting), kill the jobs via

```
condor_rm $(whoami)
```

and send an email so that we can redistribute your load.

The watch command will keep your terminal busy until the jobs are complete. If you want to get out of the screen, do 'Ctrl+A+D'. To get back in, do 'screen -ls; screen -x [SCREEN CODE from ls]'.


6. Finally, submit your actual batch, again, inside the screen:

```
python submit_jobs.py --tag 201226 --gridpack_dir=/afs/cern.ch/work/u/usarica/public/Offshell_2l2nu (/home/users/usarica/work/GenStudies/Offshell2020_Gridpacks on UCSD) \
  --condor_site=t2.ucsd.edu (or iihe.ac.be) --condor_outdir=/hadoop/cms/store/user/$(whoami)/Offshell_2L2Nu/PrivateMC (or /pnfs/iihe/cms/store/user/$(whoami)/Offshell_2L2Nu/PrivateMC) \
  submission_ZZ2L2Nu_201222_$(whoami).csv --watch_email="[YOUR EMAIL]"
```

Here, the line assumes that the output of 'whoami' is the same as your Grid user name (this is most often the case). If not, please adjust the command appropriately.

Please try to keep the tag to be the same over all users so that it becomes easier to collect various folders from different users (because you used '$(whoami)' in the --condor_outdir option) into a single one.
However, there could of course be legitimate reasons to make it different such as faulty submissions.

The persistent screen will ensure that the watch script works. That script will send you messages if soemthing goes wrong, if your credentials are going to expire, or if the jobs complete.
It will also give you a daily report of failed jobs, if any. Please be advised that it is not perfect, so you may have to log back in (to the same machine!) and the same screen to check if it is still running.
You may contact for further assistance if needed.

Good luck!
