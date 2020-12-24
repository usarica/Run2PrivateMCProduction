#!/bin/env python

import sys
import imp
import copy
import os
import filecmp
import shutil
import pickle
import math
import pprint
import subprocess
from datetime import date
from optparse import OptionParser


class BatchManager:
   def __init__(self):
      # define options and arguments ====================================
      self.parser = OptionParser()

      self.parser.add_option("--batchqueue", type="string", help="Batch queue")
      self.parser.add_option("--batchscript", type="string", help="Name of the HTCondor script")
      self.parser.add_option("--nevents", type="int", help="Number of events requested")
      self.parser.add_option("--seed", type="int", help="Event generation seed")
      self.parser.add_option("--upload", type="string", action="append", default=[], help="Uploads to the job (can specify multiple inputs)")
      self.parser.add_option("--outdir", type="string", help="Name of the output directory")

      self.parser.add_option("--condorsite", type="string", help="Name of the HTCondor site")
      self.parser.add_option("--condoroutdir", type="string", help="Name of the HTCondor output directory")

      self.parser.add_option("--outlog", type="string", help="Name of the output log file")
      self.parser.add_option("--errlog", type="string", help="Name of the output error file")

      self.parser.add_option("--required_memory", type="string", default="2048M", help="Required RAM for the job")
      self.parser.add_option("--required_ncpus", type="int", default=2, help="Required number of CPUs for the job")
      self.parser.add_option("--job_flavor", type="string", default="tomorrow", help="Time limit for job (tomorrow = 1 day, workday = 8 hours, see https://batchdocs.web.cern.ch/local/submit.html#job-flavours for more)")
      self.parser.add_option("--sites", type="string", help="Name of the HTCondor run sites")

      self.parser.add_option("--dry", dest="dryRun", action="store_true", default=False, help="Do not submit jobs, just set up the files")

      (self.opt,self.args) = self.parser.parse_args()
      optchecks=[
         "batchqueue",
         "batchscript",
         "nevents",
         "seed",
         "outdir",
         "sites",
         "condorsite",
         "condoroutdir",
         "outlog",
         "errlog",
      ]
      for theOpt in optchecks:
         if not hasattr(self.opt, theOpt) or getattr(self.opt, theOpt) is None:
            sys.exit("Need to set --{} option".format(theOpt))

      if self.opt.outdir.startswith("./"):
         self.opt.outdir = self.opt.outdir.replace(".",os.getcwd(),1)

      if not os.path.isfile(self.opt.batchscript):
         print("Batch script does not exist in current directory, will search for CMSSW_BASE/bin")
         if os.path.isfile(os.getenv("CMSSW_BASE")+"/bin/"+os.getenv("SCRAM_ARCH")+"/"+self.opt.batchscript):
            self.opt.batchscript = os.getenv("CMSSW_BASE")+"/bin/"+os.getenv("SCRAM_ARCH")+"/"+self.opt.batchscript
            print("\t- Found the batch script")
         else:
            sys.exit("Batch script {} does not exist. Exiting...".format(self.opt.batchscript))

      for fname in self.opt.upload:
         if not os.path.isfile(fname):
            sys.exit("Uploaded file {} does not exist. Exiting...".format(fname))
      self.uploads = " ".join(self.opt.upload)
      if not self.uploads:
         sys.exit("You must specify extra uploads.")

      for theOpt in optchecks:
         print("Option {}={}".format(theOpt,getattr(self.opt, theOpt)))

      self.submitJobs()


   def produceCondorScript(self):
      currentdir = os.getcwd()
      currentCMSSWBASESRC = os.getenv("CMSSW_BASE")+"/src/" # Need the trailing '/'
      currendir_noCMSSWsrc = currentdir.replace(currentCMSSWBASESRC,'')

      gridproxy = None
      if os.getenv("X509_USER_PROXY") is None or not os.getenv("X509_USER_PROXY"):
         gridproxycheckfiles = [
            "{home}/x509up_u{uid}".format(home=os.path.expanduser("~"), uid=os.getuid()),
            "/tmp/x509up_u{uid}".format(uid=os.getuid())
            ]
         for gridproxycheckfile in gridproxycheckfiles:
            if os.path.exists(gridproxycheckfile):
               gridproxy = gridproxycheckfile
      else:
         gridproxy = os.getenv("X509_USER_PROXY")
      if gridproxy is None or not os.path.exists(gridproxy):
         sys.exit("Cannot find a valid grid proxy")

      hostname = socket.gethostname()
      strrequirements = ""
      if "t2.ucsd.edu" in hostname:
         strrequirements = 'Requirements = ((HAS_SINGULARITY=?=True) && (HAS_CVMFS_cms_cern_ch =?= true)) || (regexp("(uaf-[0-9]{{1,2}}|uafino)\.", TARGET.Machine) && !(TARGET.SlotID>(TotalSlots<14 ? 3:7) && regexp("uaf-[0-9]", TARGET.machine)))'
      else:
         strrequirements = 'Requirements = (OpSysAndVer =?= "SLCern6" || OpSysAndVer =?= "SL6" || OpSysAndVer =?= "SLFermi6") || (HAS_SINGULARITY =?= true || GLIDEIN_REQUIRED_OS =?= "rhel6") || (OSGVO_OS_STRING =?= "RHEL 6" && HAS_CVMFS_cms_cern_ch =?= true)'

      scriptargs = {
         "batchScript" : self.opt.batchscript,
         "GRIDPROXY" : gridproxy,
         "SITES" : self.opt.sites,
         "CONDORSITE" : self.opt.condorsite,
         "CONDOROUTDIR" : self.opt.condoroutdir,
         "outDir" : self.opt.outdir,
         "outLog" : self.opt.outlog,
         "errLog" : self.opt.errlog,
         "QUEUE" : self.opt.batchqueue,
         "SINGULARITYVERSION" : singularityver,
         "CMSSWVERSION" : os.getenv("CMSSW_VERSION"),
         "SCRAMARCH" : scramver,
         "SUBMITDIR" : currendir_noCMSSWsrc,
         "UPLOADS" : self.uploads,
         "NEVTS" : self.opt.nevents,
         "SEED" : self.opt.seed,
         "NCPUS" : self.opt.required_ncpus,
         "REQMEM" : self.opt.required_memory,
         "JOBFLAVOR" : self.opt.job_flavor,
         "REQUIREMENTS" : strrequirements
      }

      scriptcontents = """
universe={QUEUE}
+DESIRED_Sites="{SITES}"
executable              = {batchScript}
arguments               = {SUBMITDIR} {NEVTS} {SEED} {NCPUS} {CONDORSITE} {CONDOROUTDIR}
Initialdir              = {outDir}
output                  = {outLog}.$(ClusterId).$(ProcId).txt
error                   = {errLog}.$(ClusterId).$(ProcId).err
log                     = $(ClusterId).$(ProcId).log
request_memory          = {REQMEM}
request_cpus            = {NCPUS}
+JobFlavour             = "{JOBFLAVOR}"
x509userproxy           = {GRIDPROXY}
#https://www-auth.cs.wisc.edu/lists/htcondor-users/2010-September/msg00009.shtml
periodic_remove         = JobStatus == 5
transfer_executable=True
transfer_input_files    = {UPLOADS}
transfer_output_files = ""
+Owner = undefined
+project_Name = "cmssurfandturf"
notification = Never
should_transfer_files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
{REQUIREMENTS}
+SingularityImage = "/cvmfs/singularity.opensciencegrid.org/cmssw/{SINGULARITYVERSION}"


queue

"""
      scriptcontents = scriptcontents.format(**scriptargs)

      self.condorScriptName = "condor.sub"
      condorScriptFile = open(self.opt.outdir+"/"+self.condorScriptName,'w')
      condorScriptFile.write(scriptcontents)
      condorScriptFile.close()


   def submitJobs(self):
      self.produceCondorScript()

      jobcmd = "cd {}; condor_submit {}; cd -".format(self.opt.outdir, self.condorScriptName)
      if self.opt.dryRun:
         print("Job command: '{}'".format(jobcmd))
      else:
         ret = os.system(jobcmd)



if __name__ == '__main__':
   batchManager = BatchManager()
