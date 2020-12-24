import os
import sys
import socket
import csv
import glob
import re
import subprocess
from pprint import pprint
import argparse

def run(csvs, tag, gridpack_dir, fragment_dir, direct_submit, condor_site, condor_outdir, doTestRun):

    grid_user = subprocess.check_output("voms-proxy-info -identity | cut -d '/' -f6 | cut -d '=' -f2", shell=True)
    if not grid_user:
       grid_user = os.environ.get("USER")
    grid_user = grid_user.strip()

    scram_arch = os.getenv("SCRAM_ARCH")
    cmssw_version = os.getenv("CMSSW_VERSION")
    allowed_sites = None
    if "t2.ucsd.edu" in socket.gethostname():
      allowed_sites = "T2_US_UCSD,T2_US_Caltech,T2_US_MIT,T2_US_Purdue,T2_US_Wisconsin,T2_US_Nebraska,T3_US_UCR,T3_US_Baylor,T3_US_Colorado,T3_US_NotreDame,T3_US_Cornell,T3_US_Rice,T3_US_Rutgers,T3_US_UCD,T3_US_TAMU,T3_US_TTU,T3_US_FIU,T3_US_FIT,T3_US_UMD,T3_US_OSU,T3_US_OSG,T3_US_UMiss,T3_US_PuertoRico"
    else:
      allowed_sites = "T2_CH_CERN,T2_BE_IIHE,T2_CN_Beijing,T2_RU_IHEP,T2_BE_UCL,T2_AT_Vienna,T2_BR_SPRACE,T2_BR_UERJ,T2_CH_CSCS,T2_DE_DESY,T2_DE_RWTH,T2_EE_Estonia,T2_ES_CIEMAT,T2_ES_IFCA,T2_FI_HIP,T2_FR_CCIN2P3,T2_FR_GRIF_IRFU,T2_FR_GRIF_LLR,T2_FR_IPHC,T2_GR_Ioannina,T2_HU_Budapest,T2_IN_TIFR,T2_IT_Bari,T2_IT_Legnaro,T2_IT_Pisa,T2_IT_Rome,T2_KR_KNU,T2_PK_NCP,T2_PL_Swierk,T2_PL_Warsaw,T2_PT_NCG_Lisbon,T2_RU_INR,T2_RU_ITEP,T2_RU_JINR,T2_RU_PNPI,T2_RU_RRC_KI,T2_RU_SINP,T2_TH_CUNSTDA,T2_TR_METU,T2_UA_KIPT,T2_UK_London_Brunel,T2_UK_London_IC,T2_UK_SGrid_Bristol,T2_UK_SGrid_RALPP,T3_CO_Uniandes,T3_FR_IPNL,T3_GR_IASA,T3_HU_Debrecen,T3_IT_Bologna,T3_IT_Napoli,T3_IT_Perugia,T3_IT_Trieste,T3_KR_KNU,T3_MX_Cinvestav,T3_RU_FIAN,T3_TW_NCU,T3_TW_NTU_HEP,T3_UK_London_QMUL,T3_UK_SGrid_Oxford,T3_CN_PKU"

    seed = 12345000
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
                gridpack = gridpack_dir + '/' + row["gridpack"]
                pythia_fragment = fragment_dir + '/' + row["pythia_fragment"]

                if not os.path.exists(gridpack):
                    raise RuntimeError("{} doesn't exist!".format(gridpack))
                if not os.path.exists(pythia_fragment):
                    raise RuntimeError("{} doesn't exist!".format(pythia_fragment))

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
                   elif process == "ZH_LNuQQ":
                      strproc="ZH_HToWWToLNuQQ_2LFilter"
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

                dataset = "/{}_M{}_{}/{}_private".format(strproc, mass, strprocapp, yeartag)

                batchqueue = "vanilla"
                reqmem = "2048M"
                condoroutdir = "{}/{}{}".format(condor_outdir, tag, dataset)
                #jobflavor = "tomorrow"
                jobflavor = "nextweek"
                reqncpus = 2
                if doTestRun:
                  jobflavor = "microcentury"

                outdir_core = os.getcwd() + "/tasks/{}".format(tag)
                outdir_main = "{}{}".format(outdir_core, dataset)
                if not os.path.isdir(outdir_main):
                   os.makedirs(outdir_main)

                runscripts = outdir_core + "/runscripts_{}.tar".format(year)
                if not os.path.exists(runscripts):
                   os.system("createPrivateMCRunScriptsTarball.sh Run2Legacy {} {}".format(year, runscripts))
                if not os.path.exists(runscripts):
                   raise RuntimeError("Failed to create {}".format(runscripts))
                # We also need to upload the Pythia fragment, but it needs to be done via symlinking for renaming purposes
                pythia_fragment_dset = "{}/fragment.py".format(outdir_main)
                if os.path.exists(pythia_fragment_dset):
                  os.unlink(pythia_fragment_dset)
                os.symlink(pythia_fragment, pythia_fragment_dset)

                batchscript = outdir_main + "/executable.sh"
                os.system("cp condor_executable.sh {}".format(batchscript))

                nchunks = nevts_total / nevts_per_job
                for ichunk in range(nchunks):
                  outdir = outdir_main + "/chunk_{}_of_{}".format(ichunk, nchunks)
                  if not os.path.isdir(outdir+"/Logs"):
                     os.makedirs(outdir+"/Logs")

                  seed = seed + 1000
                  reqdisk = max(int(1), int(float(4.2*1.5*float(nevts_per_job))/1024.))*1024
                  strreqdisk = "{}M".format(reqdisk)

                  jobargs = {
                     "BATCHQUEUE" : batchqueue,
                     "BATCHSCRIPT" : batchscript,
                     "NEVTS" : nevts_per_job,
                     "SEED" : seed,
                     "GRIDPACK" : gridpack,
                     "PYTHIA_FRAGMENT" : pythia_fragment_dset,
                     "RUNSCRIPTS" : runscripts,
                     "CONDORSITE" : condor_site,
                     "CONDOROUTDIR" : condoroutdir,
                     "OUTDIR" : outdir,
                     "OUTLOG" : "Logs/log_job",
                     "ERRLOG" : "Logs/err_job",
                     "REQMEM" : reqmem,
                     "REQDISK" : strreqdisk,
                     "REQNCPUS" : reqncpus,
                     "JOBFLAVOR" : jobflavor,
                     "SITES" : allowed_sites
                  }


                  runCmd = str(
                    "configurePrivateMCCondorJobs.py --batchqueue={BATCHQUEUE} --batchscript={BATCHSCRIPT} --forceSL6" \
                    " --nevents={NEVTS} --seed={SEED} --upload={GRIDPACK} --upload={PYTHIA_FRAGMENT} --upload={RUNSCRIPTS}" \
                    " --condorsite={CONDORSITE} --condoroutdir={CONDOROUTDIR}" \
                    " --outdir={OUTDIR} --outlog={OUTLOG} --errlog={ERRLOG} --required_memory={REQMEM} --required_ncpus={REQNCPUS} --required_disk={REQDISK} --job_flavor={JOBFLAVOR} --sites={SITES}"
                    ).format(**jobargs)
                  print(runCmd)
                  if not direct_submit:
                     runCmd = runCmd + " --dry"
                  os.system(runCmd)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("csvs", help="csv files with samples", nargs="+")
    parser.add_argument("--tag", help="Production tag", type=str, required=True)
    parser.add_argument("--gridpack_dir", help="Full path of gridpacks", type=str, required=True)
    parser.add_argument("--fragment_dir", help="Full path of Pythia fragments. Defaulted to --gridpack_dir.", type=str, required=False, default="")
    parser.add_argument("--condor_site", help="Condor site. You can specify the exact protocol and ports, or give something generic as 't2.ucsd.edu'. Check condor_executable.sh syntax.", type=str, required=True)
    parser.add_argument("--condor_outdir", help="Full path of the target main directory", type=str, required=True)
    parser.add_argument("--direct_submit", help="Submit without waiting", action='store_true', required=False, default=False)
    parser.add_argument("--testrun", help="Flag for test run", action='store_true', required=False, default=False)
    args = parser.parse_args()

    if not args.gridpack_dir.startswith('/'):
       raise RuntimeError("{} needs to be an absolute path.".format(args.gridpack_dir))
    if not args.fragment_dir:
       args.fragment_dir = args.gridpack_dir
    if not args.fragment_dir.startswith('/'):
       raise RuntimeError("{} needs to be an absolute path.".format(args.fragment_dir))
    if not args.condor_outdir.startswith('/'):
       raise RuntimeError("{} needs to be an absolute path.".format(args.condor_outdir))

    run(csvs=args.csvs, tag=args.tag, gridpack_dir=args.gridpack_dir, fragment_dir=args.fragment_dir, direct_submit=args.direct_submit, condor_site=args.condor_site, condor_outdir=args.condor_outdir, doTestRun=args.testrun)
