# Cluster and Cloud Computing Assignment 1 – Multicultural City 

## Overview
The task in is to implement a simple, parallelized application leveraging the University of Melbourne HPC facility SPARTAN.</br>
The application will use a large Twitter dataset and a grid/mesh for Sydney to identify the languages used in making Tweets. </br>
The objective is to count the number of different languages used for tweets in the given cells and the number of tweets in those languages and hence to calculate the multicultural nature of Sydney! </br>

## Data set
- tinyTwitter.json </br>
- smallTwitter.json </br>
- bigTwitter.json (10GB)</br>

## Instructions
To run our script, there are several libraries needed to install first: </br>
pandas, numpy, shaply, mpi4py</br>
Since we have written the "bigtwitter" file name within the input line, it only needs to put the `.py`, `.slurm` and big tweeter file in the same folder. </br>
Run each of the `.slurm` script by using sbatch and monitor the job process by using squeue command in sparten.</br>
