import os
import sys


def getVOMSProxy():
   gridproxy = None
   if os.getenv("X509_USER_PROXY") is None or not os.getenv("X509_USER_PROXY"):
      currentCMSSWBASESRC = os.getenv("CMSSW_BASE")+"/src"
      gridproxycheckfiles = [
         "{}/Run2PrivateMCProduction/OffshellMCProduction/test/x509up_u{uid}".format(currentCMSSWBASESRC, uid=os.getuid()),
         "{home}/x509up_u{uid}".format(home=os.path.expanduser("~"), uid=os.getuid()),
         "/tmp/x509up_u{uid}".format(uid=os.getuid())
         ]
      for gridproxycheckfile in gridproxycheckfiles:
         if os.path.exists(gridproxycheckfile):
            gridproxy = gridproxycheckfile
            break
   else:
      gridproxy = os.getenv("X509_USER_PROXY")
   if gridproxy is None or not os.path.exists(gridproxy):
      sys.exit("Cannot find a valid grid proxy")
   return gridproxy
