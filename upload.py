import numpy as np
import pandas as pd
import sys, os
import math, random
import chess, chess.pgn
import io
import argparse
from tqdm import tqdm 
from glob import glob
from google.cloud import bigquery

#Define by hang - could make a file parser but don't want to deal with type-casting
SCHEMA = [
    ('Black', "STRING"),
    ('BlackElo', "FLOAT64"),
    ('BlackRatingDiff', "FLOAT64"),
    ('BlackTitle', "STRING"),
    ('Date', "STRING"),
    ('ECO', "STRING"),
    ('Event', "STRING"),
    ('Increment', "FLOAT64"),
    ('Opening', "STRING"),
    ('Ply', "FLOAT64"),
    ('Result', "STRING"),
    ('Round', "FLOAT64"),
    ('Site', "STRING"),
    ('StartTime', "FLOAT64"),
    ('Termination', "STRING"),
    ('TimeControl', "STRING"),
    ('UTCDate', "DATE"),
    ('UTCDateTime', "DATETIME"),
    ('UTCTime', "TIME"),
    ('White', "STRING"),
    ('WhiteElo', "FLOAT64"),
    ('WhiteRatingDiff', "FLOAT64"),
    ('WhiteTitle', "STRING"),
    ('black_move10_eval_is_mate', "BOOL"),
    ('black_move10_evalaftermove', "FLOAT64"),
    ('black_move10_move', "STRING"),
    ('black_move10_timespent', "FLOAT64"),
    ('black_move1_evalaftermove', "FLOAT64"),
    ('black_move1_move', "STRING"),
    ('black_move1_timespent', "FLOAT64"),
    ('black_move2_eval_is_mate', "BOOL"),
    ('black_move2_evalaftermove',  "FLOAT64"),
    ('black_move2_move', "STRING"),
    ('black_move2_timespent', "FLOAT64"),
    ('black_move3_eval_is_mate', "BOOL"),
    ('black_move3_evalaftermove', "FLOAT64"),
    ('black_move3_move', "STRING"),
    ('black_move3_timespent', "FLOAT64"),
    ('black_move4_eval_is_mate', "BOOL"),
    ('black_move4_evalaftermove', "FLOAT64"),
    ('black_move4_move', "STRING"),
    ('black_move4_timespent', "FLOAT64"),
    ('black_move5_eval_is_mate', "BOOL"),
    ('black_move5_evalaftermove', "FLOAT64"),
    ('black_move5_move', "STRING"),
    ('black_move5_timespent', "FLOAT64"),
    ('black_move6_eval_is_mate', "BOOL"),
    ('black_move6_evalaftermove', "FLOAT64"),
    ('black_move6_move', "STRING"),
    ('black_move6_timespent', "FLOAT64"),
    ('black_move7_eval_is_mate', "BOOL"),
    ('black_move7_evalaftermove', "FLOAT64"),
    ('black_move7_move', "STRING"),
    ('black_move7_timespent', "FLOAT64"),
    ('black_move8_eval_is_mate', "BOOL"),
    ('black_move8_evalaftermove', "FLOAT64"),
    ('black_move8_move', "STRING"),
    ('black_move8_timespent', "FLOAT64"),
    ('black_move9_eval_is_mate', "BOOL"),
    ('black_move9_evalaftermove', "FLOAT64"),
    ('black_move9_move', "STRING"),
    ('black_move9_timespent', "FLOAT64"),
    ('white_move10_eval_is_mate', "BOOL"),
    ('white_move10_evalaftermove', "FLOAT64"),
    ('white_move10_move', "STRING"),
    ('white_move10_timespent', "FLOAT64"),
    ('white_move1_evalaftermove', "FLOAT64"),
    ('white_move1_move', "STRING"),
    ('white_move1_timespent', "FLOAT64"),
    ('white_move2_eval_is_mate', "BOOL"),
    ('white_move2_evalaftermove', "FLOAT64"),
    ('white_move2_move', "STRING"),
    ('white_move2_timespent', "FLOAT64"),
    ('white_move3_eval_is_mate', "BOOL"),
    ('white_move3_evalaftermove', "FLOAT64"),
    ('white_move3_move', "STRING"),
    ('white_move3_timespent', "FLOAT64"),
    ('white_move4_eval_is_mate', "BOOL"),
    ('white_move4_evalaftermove', "FLOAT64"),
    ('white_move4_move', "STRING"),
    ('white_move4_timespent', "FLOAT64"),
    ('white_move5_eval_is_mate', "BOOL"),
    ('white_move5_evalaftermove', "FLOAT64"),
    ('white_move5_move', "STRING"),
    ('white_move5_timespent', "FLOAT64"),
    ('white_move6_eval_is_mate', "BOOL"),
    ('white_move6_evalaftermove', "FLOAT64"),
    ('white_move6_move', "STRING"),
    ('white_move6_timespent', "FLOAT64"),
    ('white_move7_eval_is_mate', "BOOL"),
    ('white_move7_evalaftermove', "FLOAT64"),
    ('white_move7_move', "STRING"),
    ('white_move7_timespent', "FLOAT64"),
    ('white_move8_eval_is_mate', "BOOL"),
    ('white_move8_evalaftermove', "FLOAT64"),
    ('white_move8_move', "STRING"),
    ('white_move8_timespent', "FLOAT64"),
    ('white_move9_eval_is_mate', "BOOL"),
    ('white_move9_evalaftermove', "FLOAT64"),
    ('white_move9_move', "STRING"),
    ('white_move9_timespent', "FLOAT64")
]

def main():
    #In case this helps? 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/home/bwj21/.config/gcloud/application_default_credentials.json'

    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--csv_directory", help="Relative path on Cloud Storage to re-processed and BigQuery-ready CSVs - starting with bucket name, slash at end of directory", type=str)
    parser.add_argument("-t", "--table_name", help="Name of table (in cs145-f2022 project 3 dataset) to upload data", type=str)

    args = parser.parse_args()

    # Construct a BigQuery client object.
    client = bigquery.Client(project="cs145-f2022")

    table_id = f"cs145-f2022.project3.{args.table_name}"

    # Add functionality later for appending to existing table 

    job_config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(attribute[0], attribute[1]) for attribute in SCHEMA],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )

    uri = f"gs://{args.csv_directory}*"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

if __name__ == "__main__":
    main()

