#!/bin/bash

DEFAULT_ENV="dev"
DEFAULT_PACKAGES="vim tmux pip"
DEFAULT_PIP="taccjm"
DEFAULT_EXE="Mambaforge-pypy3-Linux-x86_64.sh"
DEFAULT_URL="https://github.com/conda-forge/miniforge/releases/latest/download"

helpFunction()
{
	echo ""
	echo "Usage: $0 [ENV] [PACKAGES] [PIP_PACKAGES] [EXE] [URL]"
	echo -e "\tENV - Name of environment to set-up. Default=$DEFAULT_ENV"
	echo -e "\tPACKAGES - Packages to set-up in environment. Default=$DEFAULT_PACKAGES"
	echo -e "\tPIP - PIP Packages to set-up in environment. Default=$DEFAULT_PIP"
	echo -e "\tEXE - Name of executable, will vary based on target architecture where installing. Default = $DEFAULT_EXE"
	echo -e "\tURL - URL to download mambaforge executable from. Default at $DEFAULT_URL"
	exit 1 # Exit script after printing help
}

log () {
  echo "$(date) | ENV_SETUP | ${1} | ${2}"
}

ENV=$1
if [ -z "$ENV" ]
then
    ENV=$DEFAULT_ENV
fi

PACKAGES=$2
if [ -z "$PACKAGES" ]
then
    PACKAGES="${DEFAULT_PACKAGES}"
fi

PIP_PACKAGES=$3
if [ -z "$PIP_PACKAGES" ]
then
    PIP_PACKAGES="${DEFAULT_PIP}"
fi

EXE=$4
if [ -z "$EXE" ]
then
    EXE=$DEFAULT_EXE
fi

URL=$5
if [ -z "$URL" ]
then
	URL=$DEFAULT_URL
fi

log INFO "Setting up $ENV with mamba packages $PACKAGES and pip packages $PIP_PACKAGES"

source ~/.bashrc
mamba --help > /dev/null
if [ $? -eq 0 ]; then
    log INFO "Found mamba"
else
    log INFO "Did not find mamba, installing.."
    wget -P $WORK $URL/$EXE
    if [ ! -f "$WORK/$EXE" ]; then
        log ERROR "Error download executable at $URL/$EXE"
        exit 1
    fi
    chmod +x $WORK/$EXE
    $WORK/$EXE -b -p $WORK/mambaforge 
    rm $WORK/$EXE

    log INFO "Activating conda/mamba shells"
    $WORK/mambaforge/bin/conda init
    $WORK/mambaforge/bin/mamba init

    log INFO "Sourcing bashrc"
    source ~/.bashrc
fi

mamba activate $ENV
if [ $? -eq 1 ]; then
    log INFO "Didn't find environment $ENV. Creating"
    mamba create -y --name $ENV
    if [ $? -eq 1 ]; then
        log ERROR "Could not create env $ENV"
        exit 1
    fi 
    mamba activate $ENV
fi
log INFO "Activated environment $ENV."


log INFO "Installing $PACKAGES in $ENV."
mamba install -y -q $PACKAGES
log INFO "Cleaning mamba env"
mamba clean --all -f -y -q
log INFO "Mamba env $ENV successfully created with packages $PACKAGES"

log INFO "Installing pip packages $PIP_PACKAGES"
pip install -q $PIP_PACKAGES
log INFO "Successfully installed pip packages"

exit 0

