# process_data.py

"""Module contains functions that processes the JSON data retrieved from the
YouTube API.

Functions include:

`load_all_json_files(folder_path)`:
    Combines multiple JSON files into one file whilst maintaining the same file and JSON structure.
    
`duration_to_hhmmss(duration)`:
    Takes the duration value and returns two values: the duration as a timedelta object and the 
    duration as a string.
    
`remove_prefixes(column_name)`:
    Removes the 'snippet.' and 'statistics.' and other prefixes from the column names.
    



"""

import pandas as pd
import datetime
from datetime import datetime as dt
import glob
import re


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




# functions that cleans the data


def remove_prefixes(column_name):
    # define the prefixes we want to remove and what we'll replace them with (nothing)
    prefixes_and_replacement = {"snippet.": "", 
                               "statistics.": "",
                               "contentDetails.": ""}
    
    for prefix, replacement in prefixes_and_replacement.items():
        if column_name.startswith(prefix):
            column_name = column_name.replace(prefix, replacement)
            
    return column_name


def duration_to_hhmmss(duration):
    """Takes in a string of YouTube's API duration 'response' and uses a regex to return two values:
        - the duration in sconds as a timedelta object
        - the duration in hh:mm:ss format.
    
    Parameters
    ----------
    duration (str):
        YouTube's API duration response which provides the duration of a 
        video as a string with the format "PT1H21M36S" for 1 hour, 21 minutes
        and 36 seconds.
        
        
    Returns
    --------
    duration_timedelta (timedelta):
        The input transformed into a timedelta objct which provids the duration in seconds.
        
    duration_string (str):
        The input transformed into a string with format: "hh:mm:ss".
        
        
    Example Use
    ------------
    Extracting duration of YouTube video:
        - Create a column that can compare the durations of the videos with:
            
                df['duration_seconds'] = df['duration'].apply(lambda x: duration_to_hhmmss(x)[0])
                
        - Create a quickly interpretable duration column with:
        
                df['duration_string'] = df['duration'].apply(lambda x: duration_to_hhmmss(x)[1])

    
    """
    
    # set duration pattern to capture the YouTube API response which gives duration in format PT1H52M21S
    # for 1 hour, 52 minutes, 21 seconds
    duration_pattern = "[A-Za-z]+(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    
    # use regex to compile the pattern
    duration_regex = re.compile(duration_pattern)
    
    # match the pattern and store the groups returned in a variable
    duration_groups = duration_regex.match(duration)
    
    # store each group in a variable
    full_response, hours, minutes, seconds = duration_groups[0], duration_groups[1], duration_groups[2], duration_groups[3]
    
    # ensure we have a non-Null value for hours, minutes, and seconds, and zero pad them
    if hours != None:
        if len(hours) == 1:
            hours = '0' + hours
            
    elif hours == None:
        hours = '00'
            
    if minutes != None:
        if len(minutes) == 1:
            minutes = '0' + minutes
                       
    elif minutes == None:
        minutes = '00'
    
    if seconds != None:
        if len(seconds) == 1:
            seconds = '0' + seconds
                          
    elif seconds == None:
        seconds = '00'
     
    
    # create datetime object
    duration_string = f"{hours}:{minutes}:{seconds}"
    
#     duration_formatted = dt.strptime(duration_string, "%H:%M:%S")
    
    # use the groups retrieved to create a time object
    duration_timedelta = datetime.timedelta(days=0, hours=int(hours), minutes=int(minutes), seconds=int(seconds))
    
    return duration_timedelta, duration_string
    




    
    
# function to deal with the comments


