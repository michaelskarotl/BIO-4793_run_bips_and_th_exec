import pandas as pd
import numpy as np
import os
import sys
import json

def read_logs(file_id):
    '''
    :file_id: str, the id of the file to read, it has the extension.log and is in json format.
    :return: pd.DataFrame, a dataframe with the logs of the file.
    
    '''
    logs = pd.read_json(file_id, lines=True)
    #print the columns of the dataframe
    

    return logs


def parse_logs_df(logs):
    '''
    :logs: pd.DataFrame, a dataframe with the logs of the file. It has the culmns endedAt, startedAt, executionARN, executionName, input, output, stateMachineArn, status, stateMachineArn, status, and analysis.
    The input, output, and analysis columns have nested dictionaries.
    :return: pd.DataFrame, a dataframe with the columns endedAt, startedAt, executionARN, executionName, input, output, stateMachineArn, status, and analysis. The input, output, and analysis columns have been parsed to extract the relevant information inot their own dataframes
    
    '''

    #parse the input column
    input_df = pd.json_normalize(logs['input'])

    #parse the output column

    if(logs.output[0] != None):
        output_df = pd.json_normalize(logs['output'])

    #parse the analysis column
    analysis_df = pd.json_normalize(logs['analysis'])

    #drop the input, output, and analysis columns from the logs dataframe

    logs = logs.drop(columns=['input', 'output', 'analysis'])

    return logs, input_df, output_df, analysis_df

def parse_jane_input_df(input_df):
    '''
    input_df: pd.DataFrame, a dataframe with the columns input.janeAdapter.assay', 'input.janeAdapter.analyte',
       'input.janeAdapter.matchType', 'input.janeAdapter.srcBucket',
       'input.janeAdapter.destBucket', 'input.janeAdapter.assets',
       'input.janeAdapter.intent', 'input.janeAdapter.orderLabId',
       'input.janeAdapter.patientId', 'input.janeAdapter.orderhubId',
       'input.janeAdapter.orderhubItemId', 'input.janeAdapter.urgency',
       'input.janeAdapter.cancerCohort', 'input.janeAdapter.tumorPercentage',
       'input.janeAdapter.isControl', 'input.janeAdapter.controlSampleType']
        
       The input.janeAdapter.assets is a list of dictionaries. We need to parse this column to extract the relevant information into its own dataframe

    '''

    #parse the assets column
    assets_df = pd.json_normalize(input_df['input.janeAdapter.assets'].explode())
    # drop the input.janeAdapter.assets column from the input_df dataframe
    input_df = input_df.drop(columns=['input.janeAdapter.assets'])

    #drop the input.janeAdapter.assets column from the input_df dataframe and bind the assets_df dataframe to it
    #input_df = input_df.drop(columns=['input.janeAdapter.assets'])
    output_df = pd.concat([input_df, assets_df], axis=1)

    
    return output_df


def parse_analysis_df(input_df):
    '''
    :input_df: pd.DataFrame, a dataframe with the columns 'id', 'orderLabId', 'assay', 'analyte', 'matchType', 'patientId',
       'cancerCohort', 'orderId', 'status', 'created', 'updated', 'runs',
       'qcStatus', 'qcExceptions', 'qcStatusId', 'qcUser',
       'isQcStatusApproved', 'orderhubId', 'orderhubItemId', 'intent',
       'flowcellIds', 'isControl', 'controlSampleType', 'isQc4', 'isolates',
       'notes', 'outputs', 'qcStatusNew', 'workflows'

       The isolates column is a list of dictionaries. We need to parse this column to extract the relevant information into its own dataframe
    :return: pd.DataFrame
    
    '''

    #parse the isolates column
    isolates_df = pd.json_normalize(input_df['isolates'].explode())
    # drop the isolates column from the input_df dataframe
    input_df = input_df.drop(columns=['isolates'])

    # concatenate the input_df and isolates_df dataframes
    ouput_df = pd.concat([input_df, isolates_df], axis=1)

    return ouput_df


def combine_parsed_dfs(jane_parsed, analysis_parsed, logs, sampleID):
    '''
    :jane_parsed: pd.DataFrame, output from the parse_jane_input_df function
    :analysis_parsed: pd.DataFrame, output from the parse_analysis_df function
    :return: pd.DataFrame, a dataframe with the columns from the jane_parsed and analysis_parsed dataframes
    
    '''

    #combine the jane_parsed and analysis_parsed dataframes
    combined_df = pd.concat([logs, jane_parsed, analysis_parsed], axis=1)

    # pivot the combined_df dataframe such that we have the columns from the jane_parsed and analysis_parsed dataframes as rows in the new dataframe
    combined_df = combined_df.T
    # make the index column called variable
    combined_df = combined_df.reset_index()
    # rename the columns to variable and value
    combined_df.columns = ['variable', sampleID]


    return combined_df

def convert_nested_strings_to_json(json_file):

    if isinstance(json_file, str):
        try:
            json_data = json.loads(json_file)
            return convert_nested_strings_to_json(json_data)
        except json.JSONDecodeError:
            return json_file
    elif isinstance(json_file, dict):
        for key, value in json_file.items():
            json_file[key] = convert_nested_strings_to_json(value)
    elif isinstance(json_file, list):
        for i, item in enumerate(json_file):
            json_file[i] = convert_nested_strings_to_json(item)
    return json_file


def combine_all_logs(dir):
    '''
    : directory of the logs: str, the directory where the logs are located
    : return: pd.DataFrame, a combined data frame in which the columns are the sampleIDs processed in the run
    '''
    
    counter = 0
    temp_df = pd.DataFrame()
    
    for file in os.listdir(dir):
        
        if file.endswith('.log'):
            file = os.path.join(dir, file)
            logs = read_logs(file)
            logs, input_df, output_df, analysis_df = parse_logs_df(logs)
            jane_parsed = parse_jane_input_df(input_df)
            analysis_parsed = parse_analysis_df(analysis_df)
            combined_df = combine_parsed_dfs(jane_parsed, analysis_parsed, logs, file)
            
            # if counter =0 then temp_df is empty and we can just assign combined_df to it
            if temp_df.empty:
                temp_df = combined_df
                counter += 1
            else:
                # left join the temp_df and combined_df dataframes on the variable column
                temp_df = temp_df.merge(combined_df, on='variable', how='left')    
                counter += 1

    # make the variable column the index
    temp_df = temp_df.set_index('variable')

    # split the column names to get the sampleID on the last part of the string

    temp_df.columns = temp_df.columns.str.split('/').str[-1]

    # put the index back as the first column
    temp_df = temp_df.reset_index()
    # name the index column variable
    temp_df = temp_df.rename(columns={'index': 'variable'})

    # filter for distinct rows
    temp_df = temp_df.drop_duplicates(subset='variable')

    # reset the index
    temp_df = temp_df.reset_index(drop=True)
    

    return temp_df

def main():
    import pandas as pd
    import numpy as np
    import os
    import sys
    import json

    # get the directory of the logs
    dir = sys.argv[1]
    # output the combined dataframe
    combined_df = combine_all_logs(dir)

    return combined_df

if __name__ == '__main__':
    # execute only if run as a script
    main()
