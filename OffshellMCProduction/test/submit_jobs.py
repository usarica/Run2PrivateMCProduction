#!/bin/env python

import os
import sys
import socket
import csv
import glob
import re
import subprocess
from pprint import pprint
import argparse
import multiprocessing as mp
from Run2PrivateMCProduction.OffshellMCProduction.getVOMSProxy import getVOMSProxy
from Run2PrivateMCProduction.OffshellMCProduction.PrivateMCCondorJobManager import BatchManager


def run_single(strcmd):
   BatchManager(strcmd)


def run(csvs, tag, gridpack_dir, fragment_dir, runscripts_tag, direct_submit, condor_site, condor_outdir, doOverwrite, doTestRun, nthreads, watch_email):
   if not os.path.exists(gridpack_dir):
      raise RuntimeError("{} doesn't exist!".format(gridpack_dir))
   if not os.path.exists(fragment_dir):
      raise RuntimeError("{} doesn't exist!".format(fragment_dir))

   gridproxy = getVOMSProxy()
   grid_user = subprocess.check_output("voms-proxy-info -identity -file={} | cut -d '/' -f6 | cut -d '=' -f2".format(gridproxy), shell=True)
   if not grid_user:
      grid_user = os.environ.get("USER")
   grid_user = grid_user.strip()

   hostname = socket.gethostname()
   allowed_sites = None
   if "t2.ucsd.edu" in hostname:
      #allowed_sites = "T2_US_UCSD,T2_US_Caltech,T2_US_MIT,T3_US_UCR,T3_US_Baylor,T3_US_Colorado,T3_US_NotreDame,T3_US_Cornell,T3_US_Rice,T3_US_Rutgers,T3_US_UCD,T3_US_TAMU,T3_US_TTU,T3_US_FIU,T3_US_FIT,T3_US_UMD,T3_US_OSU,T3_US_OSG,T3_US_UMiss,T3_US_PuertoRico"
      allowed_sites = "T2_US_UCSD,T2_US_Caltech,T2_US_MIT,T3_US_UCR,T3_US_Baylor,T3_US_Colorado,T3_US_NotreDame,T3_US_Cornell,T3_US_Rice,T3_US_Rutgers,T3_US_UCD,T3_US_TAMU,T3_US_TTU,T3_US_FIU,T3_US_FIT,T3_US_UMD,T3_US_OSU,T3_US_OSG,T3_US_UMiss,T3_US_PuertoRico"
   elif "uscms.org" in hostname:
      #allowed_sites = "T2_US_Caltech,T2_US_MIT,T3_US_UCR,T3_US_Baylor,T3_US_Colorado,T3_US_NotreDame,T3_US_Cornell,T3_US_Rice,T3_US_Rutgers,T3_US_UCD,T3_US_TAMU,T3_US_TTU,T3_US_FIU,T3_US_FIT,T3_US_UMD,T3_US_OSU,T3_US_OSG,T3_US_UMiss,T3_US_PuertoRico,T2_CH_CERN,T2_BE_IIHE,T2_CN_Beijing,T2_RU_IHEP,T2_BE_UCL,T2_AT_Vienna,T2_BR_SPRACE,T2_BR_UERJ,T2_CH_CSCS,T2_DE_DESY,T2_DE_RWTH,T2_ES_CIEMAT,T2_ES_IFCA,T2_FI_HIP,T2_FR_CCIN2P3,T2_FR_GRIF_IRFU,T2_FR_GRIF_LLR,T2_FR_IPHC,T2_GR_Ioannina,T2_HU_Budapest,T2_IN_TIFR,T2_IT_Bari,T2_IT_Legnaro,T2_KR_KNU,T2_PK_NCP,T2_PL_Swierk,T2_PL_Warsaw,T2_PT_NCG_Lisbon,T2_RU_INR,T2_RU_ITEP,T2_RU_JINR,T2_RU_PNPI,T2_RU_RRC_KI,T2_RU_SINP,T2_TH_CUNSTDA,T2_TR_METU,T2_UA_KIPT,T2_UK_London_Brunel,T2_UK_London_IC,T2_UK_SGrid_Bristol,T2_UK_SGrid_RALPP,T3_CO_Uniandes,T3_FR_IPNL,T3_GR_IASA,T3_HU_Debrecen,T3_IT_Bologna,T3_IT_Napoli,T3_IT_Perugia,T3_IT_Trieste,T3_KR_KNU,T3_MX_Cinvestav,T3_RU_FIAN,T3_TW_NCU,T3_TW_NTU_HEP,T3_UK_London_QMUL,T3_UK_SGrid_Oxford,T3_CN_PKU"
      #allowed_sites = "T2_US_Caltech,T2_US_MIT,T3_US_UCR,T3_US_Baylor,T3_US_Colorado,T3_US_NotreDame,T3_US_Cornell,T3_US_Rice,T3_US_Rutgers,T3_US_UCD,T3_US_TAMU,T3_US_TTU,T3_US_FIU,T3_US_FIT,T3_US_UMD,T3_US_OSU,T3_US_OSG,T3_US_UMiss,T3_US_PuertoRico,T2_CH_CERN,T2_BE_IIHE,T2_CN_Beijing,T2_RU_IHEP,T2_BE_UCL,T2_AT_Vienna,T2_BR_SPRACE,T2_BR_UERJ,T2_CH_CSCS,T2_DE_DESY,T2_DE_RWTH,T2_ES_CIEMAT,T2_ES_IFCA,T2_FI_HIP,T2_FR_CCIN2P3,T2_FR_GRIF_IRFU,T2_FR_GRIF_LLR,T2_FR_IPHC,T2_GR_Ioannina,T2_HU_Budapest,T2_IN_TIFR,T2_IT_Bari,T2_IT_Legnaro,T2_KR_KNU,T2_PK_NCP,T2_PL_Swierk,T2_PL_Warsaw,T2_PT_NCG_Lisbon,T2_RU_INR,T2_RU_ITEP,T2_RU_JINR,T2_RU_PNPI,T2_RU_RRC_KI,T2_RU_SINP,T2_TH_CUNSTDA,T2_TR_METU,T2_UA_KIPT,T2_UK_London_Brunel,T2_UK_London_IC,T2_UK_SGrid_Bristol,T2_UK_SGrid_RALPP,T3_CO_Uniandes,T3_FR_IPNL,T3_GR_IASA,T3_HU_Debrecen,T3_IT_Bologna,T3_IT_Napoli,T3_IT_Perugia,T3_IT_Trieste,T3_KR_KNU,T3_MX_Cinvestav,T3_RU_FIAN,T3_TW_NCU,T3_TW_NTU_HEP,T3_UK_London_QMUL,T3_UK_SGrid_Oxford,T3_CN_PKU"
      allowed_sites = "T2_US_UCSD,T2_US_Caltech,T2_US_MIT,T3_US_UCR,T3_US_Baylor,T3_US_Colorado,T3_US_NotreDame,T3_US_Cornell,T3_US_Rice,T3_US_Rutgers,T3_US_UCD,T3_US_TAMU,T3_US_UMD,T3_US_OSU,T3_US_OSG,T3_US_UMiss,T3_US_PuertoRico,T2_CH_CERN,T2_BE_IIHE,T2_CN_Beijing,T2_RU_IHEP,T2_BE_UCL,T2_DE_DESY,T2_DE_RWTH,T2_FR_CCIN2P3,T2_FR_GRIF_IRFU,T2_FR_GRIF_LLR,T2_FR_IPHC,T2_TR_METU"
   else:
      allowed_sites = "T2_CH_CERN,T2_BE_IIHE,T2_CN_Beijing,T2_RU_IHEP,T2_BE_UCL,T2_AT_Vienna,T2_BR_SPRACE,T2_BR_UERJ,T2_CH_CSCS,T2_DE_DESY,T2_DE_RWTH,T2_ES_CIEMAT,T2_ES_IFCA,T2_FI_HIP,T2_FR_CCIN2P3,T2_FR_GRIF_IRFU,T2_FR_GRIF_LLR,T2_FR_IPHC,T2_GR_Ioannina,T2_HU_Budapest,T2_IN_TIFR,T2_IT_Bari,T2_IT_Legnaro,T2_KR_KNU,T2_PK_NCP,T2_PL_Swierk,T2_PL_Warsaw,T2_PT_NCG_Lisbon,T2_RU_INR,T2_RU_ITEP,T2_RU_JINR,T2_RU_PNPI,T2_RU_RRC_KI,T2_RU_SINP,T2_TH_CUNSTDA,T2_TR_METU,T2_UA_KIPT,T2_UK_London_Brunel,T2_UK_London_IC,T2_UK_SGrid_Bristol,T2_UK_SGrid_RALPP,T3_CO_Uniandes,T3_FR_IPNL,T3_GR_IASA,T3_HU_Debrecen,T3_IT_Bologna,T3_IT_Napoli,T3_IT_Perugia,T3_IT_Trieste,T3_KR_KNU,T3_MX_Cinvestav,T3_RU_FIAN,T3_TW_NCU,T3_TW_NTU_HEP,T3_UK_London_QMUL,T3_UK_SGrid_Oxford,T3_CN_PKU"

   outdir_core = os.getcwd() + "/tasks/{}".format(tag)
   # Remove the run scripts in the main directory if they exist and overwrite is requested.
   if doOverwrite and os.path.isdir(outdir_core):
      for tmpf in os.listdir(outdir_core):
         if "runscripts" in tmpf and ".tar" in tmpf:
            os.unlink(outdir_core + '/' + tmpf)

   cmdlist = []
   for fname in csvs:
      with open(fname) as fh:
         reader = csv.DictReader(fh)
         for row in reader:
            channel = None
            process = None
            has_full_process = False
            if "full_process" in row:
               process = row["full_process"]
               has_full_process = True
               if "lhegen" not in row:
                  raise RuntimeError("The column 'lhegen' must be present in the csv file.")
            else:
               channel = row["channel"]
               process = row["short_process"]

            lhegen = None
            if "lhegen" in row:
               lhegen = row["lhegen"]

            # Skip entires that are commented out
            if channel is not None and channel.strip().startswith("#"): continue
            if process is not None and process.strip().startswith("#"): continue

            mass = None
            strappmass = ""
            if "mass" in row:
               mass = row["mass"]
               strappmass = "_M{}".format(mass)

            year = row["year"]
            nevts_total = int(row["nevts_total"])
            nevts_per_job = int(row["nevts_per_job"])
            gridpack = gridpack_dir + '/' + row["gridpack"]
            pythia_fragment = fragment_dir + '/' + row["pythia_fragment"]

            seed = 12345000

            if not os.path.exists(gridpack):
               raise RuntimeError("{} doesn't exist!".format(gridpack))
            if not os.path.exists(pythia_fragment):
               raise RuntimeError("{} doesn't exist!".format(pythia_fragment))

            strtune_sqrts = None
            yeartag=""
            strforcesl = None
            if runscripts_tag == "Run2Legacy":
               strforcesl = "--forceSL6"
               if year == "2016":
                  strtune_sqrts = "TuneCUETP8M1_13TeV"
                  yeartag="RunIISummer16MiniAODv3-PUMoriond17_94X_mcRun2_asymptotic_v3-v2"
               elif year == "2017":
                  strtune_sqrts = "TuneCP5_13TeV"
                  yeartag="RunIIFall17MiniAODv2-PU2017_12Apr2018_94X_mc2017_realistic_v14-v2"
               elif year == "2018":
                  strtune_sqrts = "TuneCP5_13TeV"
                  yeartag="RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15-v2"
            elif runscripts_tag == "Run2UL":
               strforcesl = "--forceSL7"
               strtune_sqrts = "TuneCP5_13TeV"
               if year == "2016_preVFP":
                  yeartag="RunIISummer20UL16MiniAODAPVv2-106X_mcRun2_asymptotic_preVFP_v11-v2"
               elif year == "2016_postVFP":
                  yeartag="RunIISummer20UL16MiniAODv2-106X_mcRun2_asymptotic_v17-v2"
               elif year == "2017":
                  yeartag="RunIISummer20UL17MiniAODv2-106X_mc2017_realistic_v9-v2"
               elif year == "2018":
                  yeartag="RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v2"
            elif runscripts_tag == "Run3Prompt":
               strforcesl = "--forceSL8"
               strtune_sqrts = "TuneCP5_13p6TeV"
               if year == "2022_preEEFix":
                  yeartag="Run3Summer22MiniAODv4-130X_mcRun3_2022_realistic_v5-v2"
            if strtune_sqrts is None:
               raise RuntimeError("Tune information is not known for tag {} and period {}.".format(runscripts_tag, year))
            if not yeartag:
               raise RuntimeError("The period tag is not defined for the period {}.".format(year))
            if not strforcesl:
               raise RuntimeError("SL version unspecified for tag {}.".format(runscripts_tag))

            strproc=""
            strprocapp=""
            if has_full_process:
               strproc = process
               strprocapp = lhegen
            else:
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
                  elif process == "WminusH_2L2Q":
                     strproc="WminusH_HToZZTo2L2Q"
                     strprocapp="powheg2-minlo-HWJ_JHUGenV735_pythia8"
                  elif process == "WplusH_2L2Q":
                     strproc="WplusH_HToZZTo2L2Q"
                     strprocapp="powheg2-minlo-HWJ_JHUGenV735_pythia8"
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
                  elif process == "ZH_LNuQQ_2LFilter":
                     strproc="ZH_HToWWToLNuQQ_2LFilter"
                     strprocapp="powheg2-minlo-HZJ_JHUGenV735_pythia8"
               elif channel == "ZPrimeToMuMuSB":
                  strproc="ZPrimeToMuMuSB"
                  if process == "NNPDF30LO4F_NoMatching_NoPSWgts":
                     strprocapp="madgraph_pythia8_NoPSWgts"
                  elif process == "Allanach_Y3_NNPDF31LO5F_NoMatching_NoPSWgts":
                     strprocapp="Allanach_Y3_5f_madgraph_pythia8_NoPSWgts"
                  elif process == "Allanach_DY3_NNPDF31LO5F_NoMatching_NoPSWgts":
                     strprocapp="Allanach_DY3_5f_madgraph_pythia8_NoPSWgts"
                  elif process == "Allanach_DYp3_NNPDF31LO5F_NoMatching_NoPSWgts":
                     strprocapp="Allanach_DYp3_5f_madgraph_pythia8_NoPSWgts"
                  elif process == "Allanach_B3mL2_NNPDF31LO5F_NoMatching_NoPSWgts":
                     strprocapp="Allanach_B3mL2_5f_madgraph_pythia8_NoPSWgts"
                  elif process == "BFF":
                     strprocapp="BFF_madgraphMLM_pythia8"
                  elif process == "BFF_LOBSM":
                     strprocapp="BFF_LOBSM_madgraphMLM_pythia8"
               elif channel == "ZPrimeToTauTauSB":
                  strproc="ZPrimeToTTSB"
                  if process == "NNPDF30LO4F_NoMatching_NoPSWgts":
                     strprocapp="madgraph_pythia8_NoPSWgts"
                  elif process == "Allanach_Y3_NNPDF31LO5F_NoMatching_NoPSWgts":
                     strprocapp="Allanach_Y3_5f_madgraph_pythia8_NoPSWgts"
                  elif process == "Allanach_DY3_NNPDF31LO5F_NoMatching_NoPSWgts":
                     strprocapp="Allanach_DY3_5f_madgraph_pythia8_NoPSWgts"
                  elif process == "Allanach_DYp3_NNPDF31LO5F_NoMatching_NoPSWgts":
                     strprocapp="Allanach_DYp3_5f_madgraph_pythia8_NoPSWgts"
                  elif process == "Allanach_B3mL2_NNPDF31LO5F_NoMatching_NoPSWgts":
                     strprocapp="Allanach_B3mL2_5f_madgraph_pythia8_NoPSWgts"
                  elif process == "BFF":
                     strprocapp="BFF_madgraphMLM_pythia8"
                  elif process == "BFF_LOBSM":
                     strprocapp="BFF_LOBSM_madgraphMLM_pythia8"
            if not strproc or not strprocapp:
               raise RuntimeError("Process strings are not defined for {} production with {} decay.".format(process, (channel if channel is not None else "''")))

            if "tuneup" in pythia_fragment:
               strprocapp = "tuneup_" + strprocapp
            elif "tunedn" in pythia_fragment:
               strprocapp = "tunedown_" + strprocapp
            elif "scaleup" in pythia_fragment:
               strprocapp = "scaleup_" + strprocapp
            elif "scaledn" in pythia_fragment:
               strprocapp = "scaledown_" + strprocapp

            strprocapp = "{}_{}".format(strtune_sqrts, strprocapp)
            dataset = "/{}{}_{}/{}_private".format(strproc, strappmass, strprocapp, yeartag)

            batchqueue = "vanilla"
            reqmem = "2048M"
            condoroutdir = "{}/{}{}".format(condor_outdir, tag, dataset)
            #jobflavor = "tomorrow"
            jobflavor = "nextweek"
            reqncpus = 2
            if channel is not None and (channel == "ZPrimeToMuMuSB" or channel == "ZPrimeToTauTauSB"):
               jobflavor = "testmatch"
            if doTestRun:
               jobflavor = "microcentury"
               reqncpus = 1
               reqmem = "1024M"

            outdir_main = "{}{}".format(outdir_core, dataset)
            if not os.path.isdir(outdir_main):
               os.makedirs(outdir_main)

            runscripts = outdir_core + "/runscripts_{}.tar".format(year)
            if not os.path.exists(runscripts):
               os.system("createPrivateMCRunScriptsTarball.sh {} {} {}".format(runscripts_tag, year, runscripts))
            if not os.path.exists(runscripts):
               raise RuntimeError("Failed to create {}".format(runscripts))
            # We also need to upload the Pythia fragment, but it needs to be done via symlinking for renaming purposes
            pythia_fragment_dset = "{}/fragment.py".format(outdir_main)
            if os.path.exists(pythia_fragment_dset):
               os.unlink(pythia_fragment_dset)
            os.symlink(pythia_fragment, pythia_fragment_dset)

            batchscript = outdir_main + "/executable.sh"
            sourcescript = "condor_executable_{}.sh".format(strforcesl.replace("--force","").lower())
            if doOverwrite or not os.path.exists(batchscript):
               os.system("cp {} {}".format(sourcescript, batchscript))

            nchunks = int(nevts_total / nevts_per_job)
            nevts_remainder = int(nevts_total - nchunks*nevts_per_job)
            if nevts_remainder>0:
               print("The total number of events for {} is not divisible without remainders to the number of events per job. The remainder will be distributed equally among {} jobs.".format(dataset, nchunks))
            reqdisk = max(int(1), int(float(4.2*1.5*float(nevts_per_job))/1024.))*1024
            if float(4.2*1.5*float(nevts_per_job))<float(reqdisk)/2.:
               reqdisk = max(512, int(float(4.2*1.5*float(nevts_per_job))+0.5))
            strreqdisk = "{}M".format(reqdisk)

            for ichunk in range(nchunks):
               seed = seed + 1000

               outdir = outdir_main + "/chunk_{}_of_{}".format(ichunk, nchunks)
               if not doOverwrite and os.path.exists(outdir+".tar"):
                  continue
               if not os.path.isdir(outdir+"/Logs"):
                  os.makedirs(outdir+"/Logs")
               elif not doOverwrite:
                  continue

               nevts_requested = nevts_per_job + min(ichunk+1, nevts_remainder) - min(ichunk, nevts_remainder)
               jobargs = {
                  "BATCHQUEUE" : batchqueue,
                  "BATCHSCRIPT" : batchscript,
                  "NEVTS" : nevts_requested,
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
                  "SITES" : allowed_sites,
                  "FORCESL" : strforcesl
                  }
               runCmd = str(
                  "--batchqueue={BATCHQUEUE} --batchscript={BATCHSCRIPT} {FORCESL}" \
                  " --nevents={NEVTS} --seed={SEED} --upload={GRIDPACK} --upload={PYTHIA_FRAGMENT} --upload={RUNSCRIPTS}" \
                  " --condorsite={CONDORSITE} --condoroutdir={CONDOROUTDIR}" \
                  " --outdir={OUTDIR} --outlog={OUTLOG} --errlog={ERRLOG} --required_memory={REQMEM} --required_ncpus={REQNCPUS} --required_disk={REQDISK} --job_flavor={JOBFLAVOR} --sites={SITES}"
                  ).format(**jobargs)
               #print(runCmd)
               if not direct_submit:
                  runCmd = runCmd + " --dry"
               cmdlist.append(runCmd)

   nthreads = min(nthreads, mp.cpu_count())
   print("Running job preparation over {} threads.".format(nthreads))
   pool = mp.Pool(nthreads)
   [ pool.apply_async(run_single, args=(strcmd,)) for strcmd in cmdlist ]
   pool.close()
   pool.join()

   if watch_email is not None:
      print("CondorWatch is going to be set up now. Be advised that the watch will not end until the jobs are complete!")
      os.system("watchPrivateMCJobs.sh tasks/{} {}".format(tag, watch_email))


if __name__ == "__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("csvs", help="csv files with samples", nargs="+")
   parser.add_argument("--tag", help="Production tag", type=str, required=True)
   parser.add_argument("--gridpack_dir", help="Full path of gridpacks", type=str, required=True)
   parser.add_argument("--fragment_dir", help="Full path of Pythia fragments. Defaulted to --gridpack_dir.", type=str, required=False, default="")
   parser.add_argument("--condor_site", help="Condor site. You can specify the exact protocol and ports, or give something generic as 't2.ucsd.edu'. Check condor_executable.sh syntax.", type=str, required=True)
   parser.add_argument("--condor_outdir", help="Full path of the target main directory", type=str, required=True)
   parser.add_argument("--direct_submit", help="Submit without waiting", action='store_true', required=False, default=False)
   parser.add_argument("--runscripts_tag", help="Run scripts tag (hint: path should look like uploads/[tag])", type=str, required=True)
   parser.add_argument("--overwrite", help="Flag to overwrite job directories even if they are present", action='store_true', required=False, default=False)
   parser.add_argument("--testrun", help="Flag for test run", action='store_true', required=False, default=False)
   parser.add_argument("--nthreads", help="Number of threads to run for this submission script", type=int, default=1, required=False)
   parser.add_argument("--watch_email", help="Email address to launch watching directly. You may always set it up separately. Automatic watching can be set up through this script if the user specifies an address, but make sure to run this script on a screen.", type=str, required=False, default=None)
   args = parser.parse_args()

   if not args.gridpack_dir.startswith('/'):
      raise RuntimeError("{} needs to be an absolute path.".format(args.gridpack_dir))
   if not args.fragment_dir:
      args.fragment_dir = args.gridpack_dir
   if not args.fragment_dir.startswith('/'):
      raise RuntimeError("{} needs to be an absolute path.".format(args.fragment_dir))
   if not args.condor_outdir.startswith('/'):
      raise RuntimeError("{} needs to be an absolute path.".format(args.condor_outdir))
   if args.watch_email is not None and (not args.watch_email or not('@' in args.watch_email)):
      raise RuntimeError("You must put a valid email address.")

   run(
      csvs=args.csvs, tag=args.tag,
      gridpack_dir=args.gridpack_dir, fragment_dir=args.fragment_dir,
      runscripts_tag=args.runscripts_tag,
      direct_submit=args.direct_submit, condor_site=args.condor_site, condor_outdir=args.condor_outdir,
      doOverwrite=args.overwrite,
      doTestRun=args.testrun,
      nthreads=args.nthreads,
      watch_email=args.watch_email
      )
