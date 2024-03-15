#!/usr/bin/env Rscript
#####################################
# Project Begin: March 12, 2024
# Project End: 
# Author: Michael Skaro
# Purpose: Use the skeleton script to create a helper script to create a helper script for querying rna val DB for samples with gcp storage links, then format
# a BIPS workflow input file. 
# rna_val_db in the /Users/michael.skaro/Research/tempusRepos/bioinf-rna-onco-verification/device_validation/rnaval_db/rnaval_data/rnaval.db
# directory. 
# Functions:
#   1. Load the database with RSQL-lite
#   2. filter the database for samples in the cancer type and with fastq files sotred on the gcp storage bucket
#   3. create a BIPS workflow input file that outlines the samples to be processed and the resources needed to run the workflow.  
# Usage: R script to be invoked and interacted with from the terminal.
# Parameters: 
# Rscript <script_name> <cancer_type> <project_id>
# ticket ID: "BFXA-4210_RNA-val_DB_to_rad-study-x_DPs"
# Note: Once the output csv has been accepted by the product team, the output will be to append the table onto the 
# Things to note: UHR = 6 well-known gene fusions used for our benchmarking study on short-read sequencing data include BCAS4-BCAS3, BCR-ABL1, ARFGEF2-SULF2, RPS6KB1-TMEM49(VMP1), TMPRSS2-ERG, and GAS6-RASA3.
# Outputs: output a flat file to the output directory, an renv.lock and a session info file to the output directory

# install the renv package if not already installed
if (!requireNamespace("renv", quietly = TRUE)) {
  install.packages("renv", repos = "http://cran.us.r-project.org")
}

if (file.exists("renv.lock")) {
  library(renv)
  renv::restore()
  # Call the libraries
  library(renv)
  library(optparse)
  library(tidyverse)
  library(data.table)
  library(DBI)
  library(RSQLite)
  library(languageserver)
  # included in tidyverse install
  library(stringr)
  library(dplyr, warn.conflicts = FALSE)
  library(dbplyr)
  library(knitr)
  library(devtools)
}

# if there is no renv.lock file, create a new project library and snapshot the R environment
if (!file.exists("renv.lock")) {
  renv::init()
}

print("Installing the necessary packages with renv loaded, this takes a while the first time, go get some coffee []D")

# install the optparse package if not already installed
if (!requireNamespace("optparse", quietly = TRUE)) {
  install.packages("optparse", repos = "http://cran.us.r-project.org")
}

# install the libraries from the cran repo and the bioconductor repo to conduct the differential expression analysis in a nexflow pipeline

# install the tidyverse package if not already installed
if (!requireNamespace("tidyverse", quietly = TRUE)) {
  install.packages("tidyverse", repos = "http://cran.us.r-project.org")
}

# install data.table package if not already installed
if (!requireNamespace("data.table", quietly = TRUE)) {
  install.packages("data.table", repos = "http://cran.us.r-project.org")
}

# install DBI package if not already installed
if (!requireNamespace("DBI", quietly = TRUE)) {
  install.packages("DBI", repos = "http://cran.us.r-project.org")
}

# install languageserver package if not already installed
if (!requireNamespace("languageserver", quietly = TRUE)) {
  install.packages("languageserver", repos = "http://cran.us.r-project.org")
}

# install devtools package if not already installed
if (!requireNamespace("devtools", quietly = TRUE)) {
  install.packages("devtools", repos = "http://cran.us.r-project.org")
}

# install knitr package if not already installed, not sure if this is in tidyverse
if (!requireNamespace("knitr", quietly = TRUE)) {
  install.packages("knitr", repos = "http://cran.us.r-project.org")
}

# install RSQLite package if not already installed, not sure if this is in tidyverse
if (!requireNamespace("RSQLite", quietly = TRUE)) {
  install.packages("RSQLite", repos = "http://cran.us.r-project.org")
}

# Call the libraries
library(renv)
library(optparse)
library(tidyverse)
library(data.table)
library(DBI)
library(RSQLite)
library(languageserver)
# included in tidyverse install
library(stringr)
library(dplyr, warn.conflicts = FALSE)
library(dbplyr)
library(knitr)
library(devtools)

# snapshot the R environment
renv:::snapshot()
# create an opt list for the aruguements
option_list = list(
  make_option(c("-i", "--input"), type="character", default=NULL, 
              help="input database, expected .db file", metavar="character"),
  make_option(c("-c", "--cancer_cohort"), type="character", default=NULL, 
              help="expected cancer type", metavar="character"),
  # make_option(c("-p", "--pipeline_name"), type="character", default=NULL, 
  #             help="Which rna-pipeline are you prepping the files", metavar="character"),
  # make_option(c("-w", "--work_flow"), type="character", default=NULL, 
  #             help="Which rna-workflow are you running in the pipeline? This argument expects a json file", metavar="character"),            
  make_option(c("-o", "--out"), type="character", default="output/", 
              help="output file directory [default= %default]", metavar="character"))

# parse the output files into the options
opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# option checking 

if (is.null(opt$input)) {
  stop("Please provide an input database file")
}

if (!file.exists(opt$out)) {
  stop("Please provide a valid output directory")
}

# load globals
db_file <- opt$input
cc <- opt$cancer_cohort
out <- opt$out


# temp during development 
#db_file <- "/Users/michael.skaro/Research/tempusRepos/bioinf-rna-onco-verification/device_validation/rnaval_db/rnaval_data/db/rnaval.db"
#out <- "/Users/michael.skaro/Desktop/test_out/BFX-4793"
tables.list <- DBI::dbConnect(RSQLite::SQLite(), db_file) %>%
  dbListTables()

# 2. create data.frames from the db 
for(i in 1:length(tables.list)){
  val <- tables.list[i]
  con <- DBI::dbConnect(RSQLite::SQLite(), db_file)
  df <- con %>%
    dbReadTable(., val, tables.list[i])
  assign(tables.list[i], df)
}

# The analysis table has the analysis_id, isolate_id, assay_version, analyte, and cancer_cohort. 
# These are the pieces of information we need to the transform_overide table

# 3. filter the database for samples in the cancer type and with fastq files sotred on the gcp storage bucket
cancer_cohort <- "Prostate Cancer"
extractFiles <- function(analysis, cancer_cohort, number_of_samples){

    # first we need to check that the cancer cohort pass in at the -c flag is in the analysis table
    if (!cancer_cohort %in% unique(analysis$cancer_cohort)){
        stop("The cancer cohort you passed in is not in the analysis table")
    }
    # filter the analysis table for the cancer_cohort
    bips_input <- analysis %>%
        dplyr::arrange(id, orderhub_id, fastq_url_gcs) %>%
        dplyr::select(orderhub_id, fastq_url_gcs, cancer_cohort, assay, analyte, match_type, intent) %>%
        dplyr::filter(cancer_cohort == "Prostate Cancer") %>%
        dplyr::sample_n(number_of_samples)
        dplyr::rename("gcs_tumor_fastq_url" = "fastq_url_gcs") %>% 
        dplyr::rename("cancer_type" = "cancer_cohort")

    return(bips_input)
}

# call the function to extract the files
seed <- 19
set.seed(seed)
bips_input <- extractFiles(analysis, cc, 5)

# write the bips input file
#out <- "/Users/michael.skaro/Research/unattached_tickets/BIO-4793_run_bips_and_th_exec/results"
data.table::fwrite(x = bips_input, file = str_glue("{out}/bips_input.csv"))

# Print the file has been created in the output directory
print(str_glue("The bips input file has been created in {out} as bips_input.csv"))


# IF we are given the report table we will need to manufacture the a th-exectute file which is 
# a JSON file that will run one step in the BIPS workflow.











