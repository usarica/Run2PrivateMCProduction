#!/bin/env python

import os
import sys
import socket
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


def run(tag, gridpack_dir, direct_submit, condor_site, condor_outdir, doOverwrite, doTestRun, nthreads, watch_email):
   if not os.path.exists(gridpack_dir):
      raise RuntimeError("{} doesn't exist!".format(gridpack_dir))

   gridproxy = getVOMSProxy()
   grid_user = subprocess.check_output("voms-proxy-info -identity -file={} | cut -d '/' -f6 | cut -d '=' -f2".format(gridproxy), shell=True)
   if not grid_user:
      grid_user = os.environ.get("USER")
   grid_user = grid_user.strip()

   scram_arch = os.getenv("SCRAM_ARCH")
   cmssw_version = os.getenv("CMSSW_VERSION")
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
   for gname in os.listdir(gridpack_dir):
      dirname = gridpack_dir+'/'+gname
      gridpack = dirname+'/gridpack.tar'
      seed = 12345000

      if not os.path.exists(gridpack):
         raise RuntimeError("{} doesn't exist!".format(gridpack))

      batchqueue = "vanilla"
      reqmem = "2048M"
      condoroutdir = "{}/{}/{}".format(condor_outdir, tag, gname)
      jobflavor = "tomorrow"
      if 'NNLO' in gname:
         jobflavor = "testmatch"
      reqncpus = 1
      if doTestRun:
         jobflavor = "microcentury"
         reqncpus = 1
         reqmem = "1024M"

      outdir_main = "{}/{}".format(outdir_core, gname)
      if not os.path.isdir(outdir_main):
         os.makedirs(outdir_main)

      runscripts = outdir_core + "/runscripts_Run2.tar"
      if not os.path.exists(runscripts):
         os.system("createPrivateMCRunScriptsTarball.sh HNNLOv2 {} {}".format("Run2", runscripts))
      if not os.path.exists(runscripts):
         raise RuntimeError("Failed to create {}".format(runscripts))

      batchscript = outdir_main + "/executable.sh"
      if doOverwrite or not os.path.exists(batchscript):
         os.system("cp condor_executable_hnnlo.sh {}".format(batchscript))

      nchunks = int(100)
      reqdisk = 1024
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

         nevts_requested = 1
         jobargs = {
            "BATCHQUEUE" : batchqueue,
            "BATCHSCRIPT" : batchscript,
            "NEVTS" : nevts_requested,
            "SEED" : seed,
            "GRIDPACK" : gridpack,
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
            "--batchqueue={BATCHQUEUE} --batchscript={BATCHSCRIPT}" \
            " --nevents={NEVTS} --seed={SEED} --upload={GRIDPACK} --upload={RUNSCRIPTS}" \
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
   parser.add_argument("--tag", help="Production tag", type=str, required=True)
   parser.add_argument("--gridpack_dir", help="Full path of gridpacks", type=str, required=True)
   parser.add_argument("--condor_site", help="Condor site. You can specify the exact protocol and ports, or give something generic as 't2.ucsd.edu'. Check condor_executable.sh syntax.", type=str, required=True)
   parser.add_argument("--condor_outdir", help="Full path of the target main directory", type=str, required=True)
   parser.add_argument("--direct_submit", help="Submit without waiting", action='store_true', required=False, default=False)
   parser.add_argument("--overwrite", help="Flag to overwrite job directories even if they are present", action='store_true', required=False, default=False)
   parser.add_argument("--testrun", help="Flag for test run", action='store_true', required=False, default=False)
   parser.add_argument("--nthreads", help="Number of threads to run for this submission script", type=int, default=1, required=False)
   parser.add_argument("--watch_email", help="Email address to launch watching directly. You may always set it up separately. Automatic watching can be set up through this script if the user specifies an address, but make sure to run this script on a screen.", type=str, required=False, default=None)
   args = parser.parse_args()

   if not args.gridpack_dir.startswith('/'):
      raise RuntimeError("{} needs to be an absolute path.".format(args.gridpack_dir))
   if not args.condor_outdir.startswith('/'):
      raise RuntimeError("{} needs to be an absolute path.".format(args.condor_outdir))
   if args.watch_email is not None and (not args.watch_email or not('@' in args.watch_email)):
      raise RuntimeError("You must put a valid email address.")

   run(
      tag=args.tag,
      gridpack_dir=args.gridpack_dir,
      direct_submit=args.direct_submit,
      condor_site=args.condor_site, condor_outdir=args.condor_outdir,
      doOverwrite=args.overwrite,
      doTestRun=args.testrun,
      nthreads=args.nthreads,
      watch_email=args.watch_email
      )
