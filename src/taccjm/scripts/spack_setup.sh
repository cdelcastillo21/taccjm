#!/bin/bash

# Default values for options
INSTALL_DIR="$WORK"
OVERWRITE=false

display_help() {
  echo "Usage: $0 [OPTIONS]"
  echo
  echo "This script clones the Spack repository to the specified directory and"
  echo "adds Spack setup commands to the ~/.bashrc file if they do not already exist."
  echo
  echo "Options:"
  echo "  -d <dir>         Specify the installation directory (default: \$WORK)."
  echo "  -o               Overwrite the existing installation if it exists."
  echo "  -h, --help       Display this help message and exit."
  echo
  echo "Example:"
  echo "  $0 -d \$HOME/spack -o"
  exit 0
}

# Parse options
while getopts ":d:oh" opt; do
  case $opt in
    d) INSTALL_DIR="$OPTARG" ;;
    o) OVERWRITE=true ;;
    h) display_help ;;
    \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
    :) echo "Option -$OPTARG requires an argument." >&2; exit 1 ;;
  esac
done

echo "Installing Spack to $INSTALL_DIR"

# Check if the installation directory exists
if [ -d "$INSTALL_DIR/spack" ]; then
  if ! $OVERWRITE; then
    echo "Spack already exists in $INSTALL_DIR/spack. Use -o option to overwrite."
    exit 1
  fi
  rm -rf "$INSTALL_DIR/spack"
fi

# Clone Spack
git clone -c feature.manyFiles=true https://github.com/spack/spack.git "$INSTALL_DIR/spack"

# Check if the block exists in ~/.bashrc
if ! grep -q ">>> spack setup" ~/.bashrc; then
  echo "Adding Spack setup to ~/.bashrc"
  {
    echo "# >>> spack setup"
    echo "alias spack-setup='source $INSTALL_DIR/spack/share/spack/setup-env.sh'"
    echo "# <<< done setup"
  } >> ~/.bashrc
else
  echo "Spack setup already exists in ~/.bashrc"
fi

