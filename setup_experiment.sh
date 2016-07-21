# Source this file to set the basic configuration needed by LArSoft 
# and for the uBooNE-specific software that interfaces to LArSoft.

FERMIAPP_LARSOFT_DIR="/grid/fermiapp/products/larsoft/"
FERMIOSG_LARSOFT_DIR="/cvmfs/fermilab.opensciencegrid.org/products/larsoft/"
OASIS_LARSOFT_DIR="/cvmfs/oasis.opensciencegrid.org/fermilab/products/larsoft/"

FERMIAPP_UBOONE_DIR="/grid/fermiapp/products/uboone/"
FERMIOSG_UBOONE_DIR="/cvmfs/uboone.opensciencegrid.org/products/"
OASIS_UBOONE_DIR="/cvmfs/oasis.opensciencegrid.org/microboone/products/"

UBOONE_BLUEARC_DATA="/uboone/data/"

# Set up ups for LArSoft
# Sourcing this setup will add larsoft and common to $PRODUCTS

for dir in $FERMIAPP_LARSOFT_DIR $FERMIOSG_LARSOFT_DIR $OASIS_LARSOFT_DIR;
do
  if [[ -f $dir/setup ]]; then
    echo "Setting up larsoft UPS area... ${dir}"
    source $dir/setup
    common=`dirname $dir`/common/db
    if [[ -d $common ]]; then
      export PRODUCTS=`dropit -p $PRODUCTS common/db`:`dirname $dir`/common/db
    fi
    break
  fi
done

# Set up ups for uBooNE

for dir in $FERMIAPP_UBOONE_DIR $FERMIOSG_UBOONE_DIR $OASIS_UBOONE_DIR;
do
  if [[ -f $dir/setup ]]; then
    echo "Setting up uboone UPS area... ${dir}"
    source $dir/setup
    break
  fi
done

# Add current working directory (".") to FW_SEARCH_PATH
#
if [[ -n "${FW_SEARCH_PATH}" ]]; then
  FW_SEARCH_PATH=`dropit -e -p $FW_SEARCH_PATH .`
  export FW_SEARCH_PATH=.:${FW_SEARCH_PATH}
else
  export FW_SEARCH_PATH=.
fi

# Add uBooNE data path to FW_SEARCH_PATH
#
if [[ -d "${UBOONE_BLUEARC_DATA}" ]]; then

    if [[ -n "${FW_SEARCH_PATH}" ]]; then
      FW_SEARCH_PATH=`dropit -e -p $FW_SEARCH_PATH ${UBOONE_BLUEARC_DATA}`
      export FW_SEARCH_PATH=${UBOONE_BLUEARC_DATA}:${FW_SEARCH_PATH}
    else
      export FW_SEARCH_PATH=${UBOONE_BLUEARC_DATA}
    fi

fi

# Set up the basic tools that will be needed
#
if [ `uname` != Darwin ]; then

  # Work around git table file bugs.

  export PATH=`dropit git`
  export LD_LIBRARY_PATH=`dropit -p $LD_LIBRARY_PATH git`
  setup git
fi
setup gitflow
setup mrb

# Define the value of MRB_PROJECT. This can be used
# to drive other set-ups. 
# We need to set this to 'larsoft' for now.

export MRB_PROJECT=larsoft

# Define environment variables that store the standard experiment name.

export JOBSUB_GROUP=uboone
export EXPERIMENT=uboone     # Used by ifdhc
export SAM_EXPERIMENT=uboone

# For Art workbook

export ART_WORKBOOK_OUTPUT_BASE=/uboone/data/users
export ART_WORKBOOK_WORKING_BASE=/uboone/app/users
export ART_WORKBOOK_QUAL="s2:e5:nu"
