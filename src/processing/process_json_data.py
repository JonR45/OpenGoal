# process_data.py

"""Module contains functions that processes the JSON data retrieved from the
YouTube API.

Functions include:

    `load_all_json_files(folder_path)`:
        Combines multiple JSON files into one file whilst maintaining the same file and JSON structure.

    `json_response_to_dataframe(json_response_data)`:
        Takes the JSON response data and turns it into a dataframe.

    `remove_prefixes(column_name)`:
        Removes the 'snippet.' and 'statistics.' and other prefixes from the column names.

    `duration_to_hhmmss(duration)`:
        Takes the duration value and returns two values: the duration as a timedelta object and the 
        duration as a string.

    `drop_columns(df)`:
        Drops the columns not needed for analysis.
    

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
    ------

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
def json_response_to_dataframe(json_response_data):
    """Takes JSON response data, performs some processing operations and outputs a dataframe.
    
    
    Processing Operations
    -----------------------
    - Grabs and stores the channel ID from the JSON response.
    - Creates empty dictionary that will house all the JSON data
    - Loops through the json_response_data and appends a dictionary of video data to the dictionary
    - Loops through the json_response_data and appends the most up to date channel statistics to
        the dictionary.
    - Create lists of the video IDs and the video dictionaries, joins these and then creates a dataframe.
    
    
    
    Parameters
    ----------
    json_response_data (json file):
        json data file. This will be all of the indivdual json files combined into one 
        file.
        
    Returns
    -------
    df_processed (dataframe):
        pandas dataframe of the JSON response data. Columns and column names are untouched.
        
    Example Use
    -----------
        
    
    Notes
    -----
    
    """
    # get channel ID by grabbing it from the first JSON file (the key of each file is the channel ID) 
    channel_id = [key for key in json_response_data[0].keys()][0]

    
    # create empty dictionary
    json_data = {}

    # create key for dictionary and give this key two values which are empty dictionaries
    json_data[channel_id] = {'channel_statistics': {}, 
                                'video_data': {}
                                }
    
    ##### Video data #####
    # loop through the json_response_data, access and then append the video data to our json_data dictionary 
    # as a value for the 'video_data' key
    for i, v in enumerate(json_response_data):

        # accss video data from the json response data
        video_data_ = json_response_data[i][channel_id]["video_data"]

        # store this in the json_data dictionary
        json_data[channel_id]["video_data"].update(video_data_)
        
      
    ##### Channel statistics #####
    # Next we need to access the channel statistics data, and then append it as a value for the 'channel_statistics' key in the 
    # json_data dictionary 
    # NOTE: the channel statistics data is NOT the same in each JSON file (the video count varies so set the video
    # count to 0 and use this as a starting value for the logic to be used in the for loop to make sure we get the 
    # most up to date channel statistics
    
    # set videoCount to 0
    json_data[channel_id]["channel_statistics"]["videoCount"] = 0

    
    # loop through the json_response_data, access channel statistics and select the one to append to dictionary
    for i, v in enumerate(json_response_data):

        # accss channel statistics from the first file in the json response data
        channel_statistics_ = json_response_data[i][channel_id]["channel_statistics"]
        # convert the string to integer
        channel_statistics_["videoCount"] = int(channel_statistics_["videoCount"])

        # add channel statistics to json_data if the video count is higher than previous file
        # NOTE: videoCount might need to be changed to viewCount as the logic could be affected if videos are
        # deleted from channel
        if channel_statistics_["videoCount"] > json_data[channel_id]["channel_statistics"]["videoCount"]:
            # update json_data dictionary with new videoCount and the up to date channel statistics
            json_data[channel_id]["channel_statistics"] = channel_statistics_
    

    ##### Create DataFrame #####
    # Using the dictionary created above with pd.json_normalize() won't work so we need to do a bit of 
    # further processing to get the data in a format that we can use pd.json_normalize()
    
    # store a list of the "video_data" dictionaries
    video_data = json_data[channel_id]["video_data"]
    # get list of video IDs and a list of the video data dictionaries dictionaries
    # store the video ids in a list
    video_ids = [vid_id for vid_id in video_data.keys()]
    video_data_dictionaries = [video_data_dict for video_data_dict in video_data.values()]
    
    # loop through the list of video data dictionaries and add the video ID to the dictionary 
    for index, data_dict in enumerate(video_data_dictionaries):
        data_dict['video_id'] = video_ids[index] 
    
    # video_data_dictionaries now has the data in a format we can use with pd.json_normalize()
    # to create a dataframe
    df = pd.json_normalize(video_data_dictionaries)
    
    
    return df




# functions that clean the data

def remove_prefixes(column_name):
    """Removes the prefixes e.g., 'snippet' and 'statistics' from the column titles.
    
    
    Parameters
    -----------
    column_name (str):
        A string representing a column name.
        
    Returns
    --------
    column_name (str):
        Returns the input without the prefixes specified in the prefixes_and_replacement
        dictionary.
        
    Example Use
    ------------
    df.columns = df.columns.map(remove_prefixes)
    
    Notes
    ------
    
    """
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



def drop_columns(df):
    """Drops columns and returns a dataframe.
    
    Parameters
    -----------
    df (dataframe):
        pandas dataframe. 
        
    Returns
    --------
    df_ (dataframe)
        dataframe with columns dropped.
    
    """
    
    # create a copy of the dataframe
    df_ = df.copy()
    
    # store the columns we want to drop in a list that we'll loop through later
    cols_to_drop = ['thumbnails.default.url','example', 'thumbnails.default.width',
                    'thumbnails.default.height', 'thumbnails.medium.url',
                    'thumbnails.medium.width', 'thumbnails.medium.height',
                    'thumbnails.high.url', 'thumbnails.high.width',
                    'thumbnails.high.height', 'thumbnails.standard.url',
                    'thumbnails.standard.width', 'thumbnails.standard.height',
                    'thumbnails.maxres.url', 'thumbnails.maxres.width',
                    'thumbnails.maxres.height', 'localized.title', 'localized.description', 
                    'defaultAudioLanguage', 'categoryId', 'liveBroadcastContent', 
                    'dimension', 'duration', 'definition', 'caption','licensedContent', 
                    'projection', 'regionRestriction.blocked', 'contentRating.ytRating']
    

    
    # loop through columns to drop list and drop if the column exists
    for column in cols_to_drop:
        try:
            df_.drop(column, axis=1, inplace=True)
            
        except KeyError as ke:
            print(ke)
            continue
    
    return df_
    


    
    
# convert to relevant data types - published at to datetime



