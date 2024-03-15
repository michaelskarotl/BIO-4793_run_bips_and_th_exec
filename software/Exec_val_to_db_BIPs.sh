# !usr/bin/bash

# This script will automate the process of checking if the files are in the enviorment I need them to be in to the process_rna_val_db_to_BIPs.R
# We will utilize the R script with 4 inputs db file, cancer type, a number of files or list of fileIDs and the output file name
# output file will be a csv. 

# This script will take in the following inputs: a db file, a cancer type, a number of files or list of fileIDs and the output file name
db_file=$1
# The second input is a string that represents the cancer type user the second and third input to determine the cancer type
cancer_type=$2" "$3
output_dir=$4
total_files=$5


# Check if the db file exists
if [ ! -f $db_file ]; then
    echo "The db file does not exist"
    exit 1
fi

# Call the R script to process the data into the BIPs format
# Rscript Process_rna_val_db_to_BIPs.R -i /Users/michael.skaro/Research/tempusRepos/bioinf-rna-onco-verification/device_validation/rnaval_db/rnaval_data/db/rnaval.db -c "Prostate Cancer" -o /Users/michael.skaro/Research/unattached_tickets/BIO-4793_run_bips_and_th_exec/results
Rscript process_rna_val_db_to_BIPs.R -i $db_file -c "$cancer_type" -o $output_dir

# # The columns of the bips_input.csv file are: orderhub_id, fastq_url_gcs, cancer_cohort, assay, analyte, match_type and intent
# # We need to use an internal tempus tool to check if the file in the fastq_url_gcs column exists in the GCS bucket, if the files 
# # in the fastq_url_gcs column do not exist in the GCS bucket, we need to perform a gcs_to_gcs transfer to move the files to the GCS bucket

#gcloud auth login
# set the project ID to to the first column in the output of the gcloud projects list command where the row contains the string "val-sequencer-output"
gcloud auth login
proj_id=$(gcloud projects list | grep "val-sequencer-output" | cut -f 1 -d " ")
gcloud config set project $proj_id

# cut the fastq_url_gcs column from the bips_input.csv file and store them in a temp file we will iterate over to check if the files exist in the GCS bucket
cut -f 2 -d "," $output_dir/bips_input.csv | awk ' {if(NR>1) print}' > temp_file.txt

# set the temp value to 0
temp=0

# Iterate over the temp file to check if the files exist in the GCS bucket
for file in $(cat temp_file.txt); do
    
    # Cut the file name into three pieces 
    # gcloud storage ls gs://tl-val-sequencer-output-fastq-us/20230321-120820-869168-bd1c760857e9/ | grep "22-B92073_RSQ5.tar.gz" | wc -l
    # gs://tl-val-sequencer-output-fastq-us/, 20230321-120820-869168-bd1c760857e9/, 22-B92073_RSQ5.tar.gz 
    # where the first piece is the bucket name, the second piece is the folder name and the third piece is the file name
    bucket=$(echo $file | cut -f 3 -d "/")
    folder=$(echo $file | cut -f 4 -d "/")
    file_name=$(echo $file | cut -f 5 -d "/")

    # Check if the file exists in the GCS bucket
    if [ $(gcloud storage ls "gs://"$bucket"/"$folder"/" | grep $file_name | wc -l) -eq 1 ]; then
        echo "\n"
        echo "the fastq tarball" $file " file"
        echo "in the" $folder "folder"
        echo "exists in the" $bucket "bucket"
        echo "\n"
        # add 1 to the temp value
        temp=$((temp+1))
    else
        echo "The file: " $file
        echo "Does not exist in the" $bucket " bucket it may need to be transferred"
        exit 1       
    fi
done

# # remove the temp file
rm temp_file.txt

# print the temp value
echo "The total number of files is: " $total_files
echo "The temp value is: " $temp

# if the temp value is equal to the total number of files in the bips_input.csv file then we can move on to the next step
if [ $temp -eq $total_files ]; then
    echo "All the files exist in the GCS bucket"
    # validate our okta credentials
    okta-personal-token get prod/Tempus-VAL
    # run the bips command
    bips $output_dir/bips_input.csv --env validaiton --log-dir ../logs/
else
    echo "Some files do not exist in the GCS"
    echo "We need to perform a gcs_to_gcs transfer to move the files to the GCS bucket"
fi
 
# okta-personal-token get preview/sundial-staging
# okta-personal-token get prod/Tempus-VAL
# okta-personal-token get okta/sundial-production

# wait for the bips command to finish, check the logs dir in ten minutes. If there are not the same number of log files as the number of files in the bips_input.csv file, then re-run the bips command

# check the logs dir in ten minutes
# if there are not the same number of log files as the number of files in the bips_input.csv file, then re-run the bips command
sleep 600 && ls -l ../logs/ | if [ $(ls -l ../logs/ | wc -l) -eq $total_files ]; then echo "The bips command has finished"; else echo bips $output_dir/bips_input.csv --env validaiton --log-dir ../logs/ fi;

# if the bips command has finished, then we can move on to the next step which is parsing the log files.
python3 combine_logs.py /Users/michael.skaro/Research/unattached_tickets/BIO-4793_run_bips_and_th_exec/logs/ /Users/michael.skaro/Research/unattached_tickets/BIO-4793_run_bips_and_th_exec/data/

# now we will get the 






