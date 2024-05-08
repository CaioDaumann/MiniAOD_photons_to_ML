#!/bin/bash

source /cvmfs/cms.cern.ch/cmsset_default.sh
cd ~/CMSSW_12_6_0/src
cmsenv
cd MiniAOD_photons_to_ML

echo 'starting python script:'
python3 preprocess.py "$@"

