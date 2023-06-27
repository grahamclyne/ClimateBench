#!/bin/bash


#SBATCH --time=16:00:00
#SBATCH --account=def-dmatthew
#SBATCH --cpus-per-task=4
#SBATCH --mem-per-cpu=16G
module load python


source /home/gclyne/projects/def-dmatthew/gclyne/cbenv/bin/activate
python3 prepare_data.py