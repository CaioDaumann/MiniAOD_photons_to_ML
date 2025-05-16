#!/bin/bash
# wrapper.sh
# -------------------
# Single‐file runner: takes one argument (the EOS path) and runs your preprocess on it.

set -exo pipefail
exec > >(tee -a wrapper_$(printf "%q" "$1" | tr '/' '_').log) 2>&1

KERB_PASS_FILE=kerberos_pass.txt
VOMS_PASS_FILE=voms_pass.txt

echo "==== STARTING SETUP ON $(hostname) @ $(date) ===="

echo "--- kinit (Kerberos) via file ---"
cat "$KERB_PASS_FILE" | kinit cdaumann@CERN.CH

echo "--- Mounting EOS ---"
eosfusebind -g

echo "--- Initializing VOMS proxy via file ---"
cat "$VOMS_PASS_FILE" | voms-proxy-init --rfc --voms cms -valid 192:00 --pwstdin

echo "--- Setting SCRAM_ARCH ---"
export SCRAM_ARCH=el9_amd64_gcc12

echo "--- Sourcing CMS environment ---"
set +u
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source /cvmfs/cms.cern.ch/cmsset_default.sh
set -u

echo "--- Entering CMSSW area and running cmsenv ---"
cd /net/data_cms3a-1/daumann/PhD/ML_reasearch/MiniAOD_photons_to_ML/CMSSW_14_1_0_pre4/src
cmsenv

# ---- now process the single file passed in ----
INPUT_FILE="$1"
echo "==== PROCESSING: $INPUT_FILE ===="
#python3 /net/data_cms3a-1/daumann/PhD/ML_reasearch/MiniAOD_photons_to_ML/preprocess_futures.py \
#    --input "$INPUT_FILE"

python3 /net/data_cms3a-1/daumann/PhD/ML_reasearch/MiniAOD_photons_to_ML/preprocess_futures.py  --file $INPUT_FILE --mode "tagprobe"  --outfile /net/data_cms3a-1/daumann/PhD/ML_reasearch/MiniAOD_photons_to_ML/test_condor_zee/ 

echo "==== FINISHED $INPUT_FILE @ $(date) ===="


