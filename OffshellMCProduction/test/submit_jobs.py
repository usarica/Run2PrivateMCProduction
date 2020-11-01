import os
import sys
import csv
import glob
import re
import subprocess
from pprint import pprint
import argparse

def run(csvs, tag, direct_submit, doTestRun):

    hadoop_user = subprocess.check_output("voms-proxy-info -identity -dont-verify-ac | cut -d '/' -f6 | cut -d '=' -f2", shell=True)
    if not hadoop_user:
       hadoop_user = os.environ.get("USER")
    hadoop_user = hadoop_user.strip()

    scram_arch = os.getenv("SCRAM_ARCH")
    cmssw_version = os.getenv("CMSSW_VERSION")

    allowed_sites="T2_US_UCSD,T2_US_Caltech,T2_US_MIT,T2_US_Purdue,T2_US_Wisconsin,T3_US_UCR,T3_US_Baylor,T3_US_Colorado,T3_US_NotreDame,T3_US_Rice,T3_US_Rutgers,T3_US_UMD,T3_US_OSU"

    seed = 12345
    for fname in csvs:
        with open(fname) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                channel = row["channel"]
                if channel.strip().startswith("#"): continue

                year = row["year"]
                process = row["short_process"]
                mass = row["mass"]
                nevts_total = int(row["nevts_total"])
                nevts_per_job = int(row["nevts_per_job"])
                gridpack = row["gridpack"]
                pythia_fragment = row["pythia_fragment"]

                if not os.path.exists(gridpack):
                    raise RuntimeError("{} doesn't exist!".format(gridpack))

                yeartag=""
                if year == "2016":
                   yeartag="RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2"
                elif year == "2017":
                   yeartag="RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2"
                elif year == "2018":
                   yeartag="RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2"

                if not yeartag: continue

                strproc=""
                strprocapp=""
                if channel == "ZZ2L2Nu":
                   if process == "GGH":
                      strproc="GluGluHToZZTo2L2Nu"
                      strprocapp="powheg2_JHUGenV735_pythia8"
                   elif process == "GGH_minloHJJ":
                      strproc="GluGluHToZZTo2L2Nu"
                      strprocapp="powheg2_minloHJJ_JHUGenV735_pythia8"
                   elif process == "VBFH":
                      strproc="VBF_HToZZTo2L2Nu"
                      strprocapp="powheg2_JHUGenV735_pythia8"
                   elif process == "WminusH":
                      strproc="WminusH_HToZZTo2L2Nu"
                      strprocapp="powheg2-minlo-HWJ_JHUGenV735_pythia8"
                   elif process == "WplusH":
                      strproc="WplusH_HToZZTo2L2Nu"
                      strprocapp="powheg2-minlo-HWJ_JHUGenV735_pythia8"
                   elif "ZH" in process:
                      strproc=process
                      strprocapp="powheg2-minlo-HZJ_JHUGenV735_pythia8"
                elif channel == "WW2L2Nu":
                   if process == "GGH":
                      strproc="GluGluHToWWTo2L2Nu"
                      strprocapp="powheg2_JHUGenV735_pythia8"
                   elif process == "GGH_minloHJJ":
                      strproc="GluGluHToWWTo2L2Nu"
                      strprocapp="powheg2_minloHJJ_JHUGenV735_pythia8"
                   elif process == "VBFH":
                      strproc="VBF_HToWWTo2L2Nu"
                      strprocapp="powheg2_JHUGenV735_pythia8"
                   elif process == "WminusH_2LOSFilter":
                      strproc="WminusH_HToWWToLNu2X_2LOSFilter"
                      strprocapp="powheg2-minlo-HWJ_JHUGenV735_pythia8"
                   elif process == "WplusH_2LOSFilter":
                      strproc="WplusH_HToWWToLNu2X_2LOSFilter"
                      strprocapp="powheg2-minlo-HWJ_JHUGenV735_pythia8"
                   elif process == "ZH":
                      strproc="ZH_HToWWTo2L2Nu"
                      strprocapp="powheg2-minlo-HZJ_JHUGenV735_pythia8"

                if "tuneup" in pythia_fragment:
                   strprocapp = "tuneup_" + strprocapp
                elif "tunedn" in pythia_fragment:
                   strprocapp = "tunedown_" + strprocapp
                elif "scaleup" in pythia_fragment:
                   strprocapp = "scaleup_" + strprocapp
                elif "scaledn" in pythia_fragment:
                   strprocapp = "scaledown_" + strprocapp

                if year == "2016":
                   strprocapp = "TuneCUETP8M1_13TeV_" + strprocapp
                else:
                   strprocapp = "TuneCP5_13TeV_" + strprocapp

                dataset = "/{}_M{}_{}/{}_private/LHE".format(strproc, mass, strprocapp, yeartag)

                batchqueue = "vanilla"
                reqmem = "2048M"
                condoroutdir = "/hadoop/cms/store/user/{}/Offshell_2L2Nu/PrivateMC/{}{}".format(hadoop_user, tag, dataset)
                condorsite = "t2.ucsd.edu"
                jobflavor = "tomorrow"

                outdir_main = os.getcwd() + "/tasks/PrivateMC/{}{}".format(tag, dataset)
                if not os.path.isdir(outdir_main):
                   os.makedirs(outdir_main)

                batchscript = outdir_main + "/executable.sh"
                os.system("cp condor_executable_LHE.sh {}".format(batchscript))

                nchunks = nevts_total / nevts_per_job
                for ichunk in range(nchunks):
                  outdir = outdir_main + "/chunk_{}_of_{}".format(ichunk, nchunks)
                  if not os.path.isdir(outdir+"/Logs"):
                     os.makedirs(outdir+"/Logs")

                  seed = seed + nevts_per_job/100

                  jobargs = {
                     "BATCHQUEUE" : batchqueue,
                     "BATCHSCRIPT" : batchscript,
                     "QUEUE" : batchqueue,
                     "NEVTS" : nevts_per_job,
                     "SEED" : seed,
                     "TARFILE" : gridpack,
                     "CONDORSITE" : condorsite,
                     "CONDOROUTDIR" : condoroutdir,
                     "OUTDIR" : outdir,
                     "OUTLOG" : "Logs/log_job",
                     "ERRLOG" : "Logs/err_job",
                     "REQMEM" : reqmem,
                     "JOBFLAVOR" : jobflavor,
                     "SITES" : allowed_sites
                  }


                  runCmd = str(
                    "configurePrivateMCCondorJobs.py --batchqueue={BATCHQUEUE} --batchscript={BATCHSCRIPT}" \
                    " --nevents={NEVTS} --seed={SEED} --tarfile={TARFILE} --condorsite={CONDORSITE} --condoroutdir={CONDOROUTDIR}" \
                    " --outdir={OUTDIR} --outlog={OUTLOG} --errlog={ERRLOG} --required_memory={REQMEM} --job_flavor={JOBFLAVOR} --sites={SITES}"
                    ).format(**jobargs)
                  print(runCmd)
                  if not direct_submit:
                     runCmd = runCmd + " --dry"
                  os.system(runCmd)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("csvs", help="csv files with samples", nargs="+")
    parser.add_argument("--tag", help="Production tag", type=str, required=True)
    parser.add_argument("--direct_submit", help="Submit without waiting", action='store_true', required=False, default=False)
    parser.add_argument("--testrun", help="Flag for test run", action='store_true', required=False, default=False)
    args = parser.parse_args()

    run(csvs=args.csvs, tag=args.tag, direct_submit=args.direct_submit, doTestRun=args.testrun)
