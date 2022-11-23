import numpy as np
import pandas as pd
import sys, os
import math, random
import chess, chess.pgn
import io
import argparse

def parse_pgn(lines, max_ply):
    """
    Generate attributes for one tuple encoding a single PGN-encoded chess game.

    Input: 
    lines - list of strings corresponding to a single PGN-encoded chess game incl. all heading lines; 
        empty lines are stripped; last line is the set of game moves in algebraic notation
    max_ply - int, maximum number of plies to get move (and clock and evaluation) features
    
    Output: 
    python dictionary with important features extracted from PGN header and game text
    """
    features = {}
    
    #Get raw heading features first
    for line in lines[:-1]:
        feature = line[1:-1].split() #remove brackets
        key = feature[0]
        value = " ".join(feature[1:])[1:-1] #get feature value, remove quotes
        features[key] = value
    
    #Get game-level features
    game = chess.pgn.read_game(io.StringIO(lines[-1]))
    features['Ply'] = game.end().ply()
    
    #####create new move-level features
    nextpos = game
    for ply in range(1,min(max_ply+1, game.end().ply())):
        player = 'white' if ply % 2 == 1 else 'black'
        movenum = math.ceil(ply/2)
        feature_name = f'{player}_move{movenum}'
        
        nextpos = nextpos.next()
        
        features[f'{feature_name}_move'] = nextpos.move.uci()
        features[f'{feature_name}_timespent'] = nextpos.clock()
        if nextpos.eval() is not None:
            features[f'{feature_name}_evalaftermove'] = nextpos.eval().white().score()
            if nextpos.eval().is_mate():
                features[f'{feature_name}_eval_is_mate'] = 1
        
        
    return features

def export(game_batch, export_path, export_name, batchnum, final_size=False):
    """
    Transform a batch of games into Pandas DataFrame and export as CSV for eventual BigQuery upload.
    
    Input: 
    game_batch - list of dictionaries corresponding to rows (games)
    export_path - path-like string for the location of exported tabular files
    export_name - string, file naming pattern for exported tabular files 
    batch_num - int ordinal identifier for the batch number out of the main file
    final_size - bool for outputting size of final (leftover) batch for verbose printing - default False
    
    Output: 
    set of dataframe column names (list if final_size=True); 
    numrows of final dataframe outputted if final_size=True

    Pandas file is also written out
    
    """
    game_df = pd.DataFrame(game_batch)
    game_df.to_csv(f'{export_path}/{export_name}_batch{batchnum}.csv')

    if final_size:
        return list(game_df.columns), game_df.shape[0]

    return set(game_df.columns)

def batch_load_files_fromconsole(export_path=None, export_name=None, max_ply=20, batch_size=100000, print_progress=True):
    """
    Accept console-streamed data from a compressed bulk PGN file and transform into tabular format
    in the form of multiple CSVs. 

    Input: 
    export_path - path-like string for the location of exported tabular files
    export_name - file naming pattern for exported tabular files (filename-like string)
    batch_size - int for number of rows in each exported Pandas DataFrame, default 100k
    print_progress - bool for printing progress to console 
    
    Output: 
    None
    
    console print options for # batches sent out

    a .txt file with all attribute names (one per line) across all batches 
    is also written out; used for creating BigQuery schema     
    """
    
    game_batch = [] #list of feature dicts to be converted to pd DataFrame
    currgame = [] #hold list of lines for game parsing
    batchnum = 1 #For naming csv files and printing progress
    schema_columns = set() #For naming 
    
    #Prepare filenames for exporting CSVs
    if export_path is None:
        export_path = '.'
    if export_name is None:
        export_name = 'lichess_games' #can't get filename from console-output version
    
    #Parse files
    line_num = 0

    for line in sys.stdin:
        if line_num < 50000000:
            line_num += 1
            continue     
        stripped = line.strip()
        if stripped == '': continue #skip blank lines

        #End-of-game condition
        elif stripped[0].isdigit():
            currgame.append(stripped)
            game_batch.append(parse_pgn(currgame, max_ply=max_ply))
            currgame = []

            #End-of-batch condition
            if len(game_batch) == batch_size:
                schema_columns = schema_columns.union(export(game_batch, export_path, export_name, batchnum))
                game_batch = []
                if print_progress: 
                    print(f'{batchnum} batches exported ({batchnum*batch_size} total rows)')
                batchnum += 1

        #Middle-of-game condition
        else:
            currgame.append(stripped)
    
    #We may have a final ragged batch to process 
    last_batch_columns, final_size = export(game_batch, export_path, export_name, batchnum, final_size=True)
    schema_columns = schema_columns.union(set(last_batch_columns))
    if print_progress: 
        print(f'{batchnum} batches exported ({(batchnum-1)*batch_size+final_size} total rows)')
    
    #Write out schema after somewhat organizing list
    #Will reorder columns manually before BigQuery upload - just for readability
    schema_columns = sorted(list(schema_columns))

    with open (f'{export_path}/{export_name}_schema.txt', 'w') as schema:
        for attribute in schema_columns:
            schema.write(attribute + '\n')
        schema.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output_path", help="path to export CSVs", type=str, default=None)
    parser.add_argument("-n", "--output_name", help="base name for CSVs (batch number appended later)", type=str, default=None)
    parser.add_argument("-p", "--max_ply", help="max number of game plies to generate move-level features", type=int, default=20)
    parser.add_argument("-s", "--batch_size", help="number of rows per batch", type=int, default=100000)
    parser.add_argument("-v", "--verbose", help="print updates for each batch", type=int, choices=[0,1], default=1)

    args = parser.parse_args()

    batch_load_files_fromconsole(export_path=args.output_path, export_name=args.output_name, max_ply=args.max_ply,
        batch_size=args.batch_size, print_progress=bool(args.verbose))



if __name__ == "__main__":
    main()