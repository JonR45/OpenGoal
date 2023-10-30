# process_data.py

"""Module contains functions that processes the JSON data retrieved from the
YouTube API.

Functions include:

`load_all_json_files(folder_path)`: Combines multiple JSON files into one file whilst 
    maintaining the same file and JSON structure.
    



"""

import pandas as pd
from datetime import datetime as dt
import glob


def load_all_json_files(folder_path):
    """Combines multiple JSON files into one file whilst maintaining the same
    file and JSON structure.
    
    Parameters
    -----------
    folder_path: str
        String of the path to the folder that contains the json files you
        want to combine into one.
    
    
    Returns
    --------
    "".json: json file
        File with all the json file contained in the folder path in one
        json file.
    
    
    Notes
    -------

    """
    
    input_path = folder_path + "/*.json"
    output_path = "../data/processed/"
    
    # create empty list - this will be populated with all the json files that we want to combine
    data = []
    
    # loop through the files in the foler and append them to the list
    for file in glob.glob(input_path):
        with open(file, "r") as f:
            data.append(json.loads(f.read()))
            
            
    # create a new timesatamped file with all the json data and ave in the output folder path
    with open(f"{output_path}/all_json_data_{dt.now().strftime('%Y-%m-%d--T%H-%M-%S')}.json", "w") as f:
        json.dump(data, f, indent=4)
    
    return


        

# function that places response into dataframe




# function that keeps only the data we want




# function that cleans the data





# function to deal with the comments


