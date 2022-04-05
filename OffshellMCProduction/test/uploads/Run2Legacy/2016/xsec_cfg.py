import FWCore.ParameterSet.Config as cms


process = cms.Process('XSec')

process.load('FWCore.MessageService.MessageLogger_cfi')
# How often do you want to see the "Begin processing the .."
process.MessageLogger.cerr.FwkReport.reportEvery = 1000

process.maxEvents = cms.untracked.PSet(
   input = cms.untracked.int32(-1),
)
process.source = cms.Source ("PoolSource",
   fileNames = cms.untracked.vstring("file://miniaod.root"),
)
process.source.duplicateCheckMode = cms.untracked.string("noDuplicateCheck")

process.xsec = cms.EDAnalyzer("GenXSecAnalyzer")

process.ana = cms.Path(process.xsec)
process.schedule = cms.Schedule(process.ana)
