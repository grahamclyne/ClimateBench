#!/bin/bash


#SBATCH --time=00:02:00
#SBATCH --account=def-dmatthew
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=16G
module load python/3.11
module load mpi4py
#module load netcdf/4.9.0
source /home/gclyne/climatebench_env/bin/activate
python prepare_data.py
