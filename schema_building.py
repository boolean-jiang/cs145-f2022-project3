import numpy as np
import pandas as pd
import sys, os
import math, random
import chess, chess.pgn
import io
import argparse
from tqdm import tqdm 
from glob import glob
from google.cloud import storage

def fix_dates_times(df):
    """Change date formats to be BigQuery compliant"""
    if "Date" in df.columns:
        df["Date"] = df.Date.apply(lambda date: date.replace('.', '-'))
    df["UTCDate"] = df.UTCDate.apply(lambda date: date.replace('.', '-'))
    df["UTCDateTime"] = df.UTCDate + " " + df.UTCTime
    return df

def create_gametime_features(df):
    """Create additional game time features in Pandas - cheaper than in SQL"""
    #Extract starting time and increment from the TimeControl column
    if "TimeControl" in df.columns:
        df["StartTime"] = df.TimeControl.apply(lambda time: time[:time.find('+')])
        df["Increment"] = df.TimeControl.apply(lambda time: time[(time.find('+')+1):])
    
    #If not a round in a tournament, replace with NULL so this column can be interpreted as int
    if "Round" in df.columns:
        df['Round'] = df.Round.replace({'-': None})

    return df

def update_feature_types(df):
    """Update all feature types to avoid BigQuery typing issues"""
    
    #Stop dealing with broken INT columns
    for col in df.columns:
        df[col] = df[col].replace({'-': None})
    for col in df.columns:
        if "evalaftermove" in col:
            df[col] = pd.to_numeric(df[col])
        elif "timespent" in col:
            df[col] = pd.to_numeric(df[col])
        elif "eval_is_mate" in col:
            df[col] = df[col].astype('bool')
        elif "RatingDiff" in col:
            df[col] = pd.to_numeric(df[col])
    
    return df

def main():
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/bwj21/.config/gcloud/application_default_credentials.json'
    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--csv_directory", help="path to stored CSVs", type=str, default='.')
    parser.add_argument("-o", "--output_directory", help="path to save updated CSVs", type=str, default=None)
    parser.add_argument("-b", "--bucket_name", help="name of Cloud Storage bucket to write to", type=str, default=None)
    parser.add_argument("-p", "--path_name", help="name of Cloud Storage path to write to", type=str, default='.')

    args = parser.parse_args()

    if args.output_directory is None:
        output_dir = args.csv_directory
    else:
        output_dir = args.output_directory

    if args.bucket_name is not None:
        storage_client = storage.Client(project="CS145-F2022")
        bucket = storage_client.bucket(args.bucket_name)

    curr_df = None

    #Hard-code in the initial features we will engineer later
    all_cols = {'UTCDateTime', 'StartTime', 'Increment'}

    #Get all possible column names
    print('Getting column names:')
    for csv_path in tqdm(glob(f'{args.csv_directory}/*.csv')):
        curr_df = pd.read_csv(csv_path, dtype='object', index_col=[0])

        all_cols = all_cols.union(set(curr_df.columns.values))
    
    #Work with list instead of set for easier sorting:
    all_cols = sorted(list(all_cols))

    #Add all possible column names to each CSV - so BigQuery doesn't handle CSVs with different schemas
    print('Rewriting old CSVs:')
    all_df = None
    for csv_path in tqdm(glob(f'{args.csv_directory}/*.csv')):
        curr_df = pd.read_csv(csv_path, dtype='object', index_col=[0])

        #Additional feature-fixing and engineering
        curr_df = fix_dates_times(curr_df)
        curr_df = create_gametime_features(curr_df)
        curr_df = update_feature_types(curr_df)
 
        for col in all_cols:
            #Assign None value to columns not natively found in a particular CSV 
            if col not in curr_df.columns.values:
                curr_df[col] = None

        #reorder columns for BigQuery consistency
        curr_df = curr_df[all_cols]

        #Write CSV with updated column version 
        if all_df is None:
            all_df = curr_df
        else:
            all_df = pd.concat([all_df, curr_df])

        filename = csv_path.split('/')[-1]

        curr_df.to_csv(f'{output_dir}/{filename}', index=False)

        if args.bucket_name is not None:
            bucket.blob(f'{args.path_name}/{filename}').upload_from_string(curr_df.to_csv(index=False), 'text/csv')
    
    # print('Saving one giant CSV:')
    # all_df.to_csv(f'{output_dir}/all_games_main.csv', index=False)
    # if args.bucket_name is not None:
    #     bucket.blob(f'{args.path_name}/all_games_main.csv').upload_from_string(all_df.to_csv(index=False), 'text/csv')
    
    #Save a final copy of the schema for use in BigQuery database-building

    print("Building final schema .txt file:")
    with open (f'{output_dir}/bigquery_schema.txt', 'w') as schema:
        for attribute in all_cols:
            schema.write(attribute + '\n')
        schema.close()
    
    print("Done!")

if __name__ == "__main__":
    main()
