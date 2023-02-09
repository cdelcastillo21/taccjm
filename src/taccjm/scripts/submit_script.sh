#!/bin/bash
#----------------------------------------------------
# {job_name} 
#
#----------------------------------------------------

#SBATCH -J {job_id}     # Job name
#SBATCH -o {job_dir}/{job_id}.o%j # Name of stdout output file
#SBATCH -e {job_dir}/{job_id}.e%j # Name of stderr error file
#SBATCH -p {queue}      # Queue (partition) name
#SBATCH -N {node_count}          # Total num nodes
#SBATCH -n {cores}          # Total num mpi tasks
#SBATCH -t {run_time}         # Run time (hh:mm:ss)
#SBATCH --ntasks-per-node {processors_per_node} # tasks per node
#SBATCH -A {allocation} # Allocation name
{extra_directives}
#----------------------------------------------------

cd {job_dir}

module load {module_list}

export NP={cores}

source ~/.bashrc

conda activate taccjm

{run_cmnd}
