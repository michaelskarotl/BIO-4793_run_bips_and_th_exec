"""
Create input.json for th_exec to run samples in batch for xt-onco val run

python3 th_adapter.py input.csv

"""

import json
import os
import subprocess
import sys
import time

import pandas as pd

# Check if the input csv file exists
if len(sys.argv) < 2 or (not sys.argv[1].endswith("csv")):
    print("Please provide the input csv file on the command line!")
    sys.exit(1)

# sample input json file
data = {
    "transform_id": "ccf09f5c-fa3a-4050-8071-5cc2ea3f4b5d",
    "data_product_manifests": {},
    "data_products": {},
    "environment": {
        "CONFIG": {
            "order_id": "POS07334-hd200-1",
            "src_bucket": "clinical-data-processing-staging",
            "dest_bucket": "clinical-data-processing-complete-staging",
            "tarball": "POS07334-hd200-1",
            "instance_type": "c5.9xlarge",
            "volume_size": "1000",
            "ref_bucket": "fda-xt-onco-refdata",
            "docker_tag": "nil",
            "tumor_fastq_archive": "gs://tl-bet-sequencer-output-fastq-us/20220307-181446-495028-746f9ed611e8/22-POS07334_DSQ1.tar.gz",
#            "normal_fastq_archive": "gs://tl-bet-sequencer-output-fastq-us/20220307-181446-495028-746f9ed611e8/22-A29776_DSQ1.tar.gz",
            "docker_image": "jane-dna-variant-fda",
            "do_upload": False,
            "do_upload_data_products": True,
            "do_upload_to_cloud": True,
            "no_upload_data_products_via_reconciler": False,
            "workflow": "tempus_xt_onco_matched",
            "token": "bioinformatics",
            "cancer_type": "Melanoma",
            "flag_post": "nil",
            "url-massarray-snp": "nil",
            "url-massarray-cnv": "nil",
            "tumor_purity_pathology": "21",
            "assay": "xT-onco.v1",
            "order-fastq-archive": "nil",
            "slack_info_channel": "bio_jane_staging",
            "slack_error_channel": "bio_jane_error_staging",
            "data_product_storage_bucket": "tsc-bioinf-data-products-staging-usw2",
            "log_formatter": "json",
        }
    },
    "parameters": {},
}

# create a directory to save input json files
if not os.path.exists("input_json"):
    os.mkdir("input_json")

# read in csv files
input_csv = pd.read_csv(sys.argv[1])

# create an empty list to save input json file names
json_name_list = []

# create json files for each row of csv
for i in range(len(input_csv)):
    temp_data = data
    temp_data["transform_id"] = input_csv.loc[i, "transform_id"]
    temp_data["environment"]["CONFIG"]["order_id"] = input_csv.loc[i, "order_id"]
    temp_data["environment"]["CONFIG"]["tarball"] = input_csv.loc[i, "order_id"]
#    temp_data["environment"]["CONFIG"]["ref_bucket"] = input_csv.loc[i, "ref_bucket"]
    temp_data["environment"]["CONFIG"]["tumor_fastq_archive"] = input_csv.loc[
        i, "tumor_fastq_archive"
    ]
 #   temp_data["environment"]["CONFIG"]["normal_fastq_archive"] = input_csv.loc[
 #       i, "normal_fastq_archive"
 #   ]
    temp_data["environment"]["CONFIG"]["docker_image"] = input_csv.loc[
        i, "docker_image"
    ]
    temp_data["environment"]["CONFIG"]["workflow"] = input_csv.loc[i, "workflow"]
    temp_data["environment"]["CONFIG"]["cancer_type"] = input_csv.loc[i, "cancer_type"]
    temp_data["environment"]["CONFIG"]["assay"] = input_csv.loc[i, "assay"]
    temp_data = [temp_data]
    json_string = json.dumps(temp_data)
    name = input_csv.loc[i, "order_id"]
    json_name_list.append("./input_json/%s.json" % name)
    with open("./input_json/%s.json" % name, "w") as outfile:
        outfile.write(json_string)

# launch each sample
for filename in json_name_list:
    subprocess.run(
        [
            "python3",
            "-m",
            "bioinf_analysis_utils.orch.th_execute",
            "--input-path",
            filename,
            "--output-path",
            "out.json",
        ]
    )
    time.sleep(1)
