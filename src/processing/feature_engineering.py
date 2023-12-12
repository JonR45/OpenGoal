# feature_engineering.py
"""Module contains functions that create columns using data that is already in the dataframe.

Functions include:

    `create_year_month_columns(df):`:
        Takes the publishedAt value and creates separate columns for year and month.
    
    `create_video_counts_columns(df)`:
        Calculates the number of videos published in each month and year and creates a column for both.

    `duration_to_hhmmss(duration)`:
        Takes the duration value and returns two values: the duration as a timedelta object and the 
        duration as a string.
    

"""


import pandas as pd
import datetime as dt

def create_year_month_columns(df):
    """Calculates the number of videos per month and per year based on the date in the 'publishedAt' column.
    
    Parameters
    -----------
    df : pandas dataframe
        Dataframe with 'publishedAt' column.
        
    Returns
    -------
    df : pandas dataframe
        The input dataframe with additional 'publishedAtYear', 'publishedAtMonth', and 'publishedAtYearMonth columns.
        
    Notes
    ------
    
    
    """
    # create copy of input dataframe to avoid any inplace changes
    df_ = df.copy()
    
    # convert publishedAt values from string to datetime
    df_['publishedAt'] = pd.to_datetime(df_['publishedAt'])
    
    # use strftime to format the datetime value into just year and month so we can group by year and month
    df_['publishedAtYear'] = df_['publishedAt'].dt.strftime("%Y")
    df_['publishedAtMonth'] = df_['publishedAt'].dt.strftime("%m")
    df_['publishedAtYearMonth'] = df_['publishedAt'].dt.strftime("%Y-%m")

    # sort values and reset index
    df_.sort_values(by=['publishedAt'], inplace=True)
    df_.reset_index(drop=True, inplace=True)

    df_ = df_[['videoID', 'publishedAt', 'publishedAtYear', 'publishedAtMonth', 'publishedAtYearMonth', 
               'channelTitle', 'channelId',  'title', 'description', 'duration_timedelta', 'duration_hhmmss', 'tags', 
               'viewCount', 'likeCount', 'favoriteCount','commentCount']]


    return df_
    
    



# define a function that counts the number of videos released in a calendar month
def create_video_counts_columns(df):
    """Calculates the number of videos per month and per year based on the date in the 'publishedAt' column.
    
    Parameters
    -----------
    df : pandas dataframe
        Dataframe with 'publishedAt' column.
        
    Returns
    -------
    df : pandas dataframe
        The input dataframe with additional 'videoCountMonth' and 'videoCountYear' columns.
        
    Notes
    ------
    
    
    """
    # create copy of input dataframe to avoid any inplace changes
    df_ = df.copy()
    
    # create video count per month dataframe
    video_count_month = df_['publishedAtYearMonth'].value_counts().reset_index()
    
    # change column titles
    video_count_month.columns = ['yearMonth', 'videoCountMonth']
    
    # convert year_month to datetime for easy sorting by date
    video_count_month['yearMonth'] = pd.to_datetime(video_count_month['yearMonth'], format="%Y-%m")
    
    # add year and month columns for easy merging later
    video_count_month['year'] = video_count_month['yearMonth'].dt.strftime("%Y")
    video_count_month['month'] = video_count_month['yearMonth'].dt.strftime("%m")
    video_count_month['yearMonth'] = video_count_month['yearMonth'].dt.strftime("%Y-%m")
    
    # sort values by date
    video_count_month.sort_values(by=["yearMonth"], inplace=True)
    video_count_month.reset_index(inplace=True, drop=True)
    
    # reorder columns
    video_count_month = video_count_month[["yearMonth", "year", "month", "videoCountMonth"]]
    

    # get video count per year
    video_count_year = df_['publishedAtYear'].value_counts().reset_index()
    
    # change column titles
    video_count_year.columns = ['year', 'videoCountYear']
    
    # sort values by year
    video_count_year.sort_values(by=["year"], inplace=True)
    video_count_year.reset_index(inplace=True, drop=True)

    # merge video_count_month and video_count_year dataframes
    video_counts = pd.merge(video_count_month, video_count_year, left_on="year", right_on="year", how='left')

    # add video count data to original dataframe
    df_ = pd.merge(df_, video_counts, left_on=['publishedAtYearMonth'], right_on=['yearMonth'], how='left')

    # drop unnecessary columns (the columns in these data is contained in publishedAtYear, publishedAtMonth etc.
    df_.drop(labels=["yearMonth", "year", "month"], axis=1, inplace=True)

    # sort values and reset index
    df_.sort_values(by=['publishedAt'], inplace=True)
    df_.reset_index(drop=True, inplace=True)

    df_ = df_[['videoID', 'publishedAt', 'publishedAtYear', 'publishedAtMonth', 'publishedAtYearMonth', 
               'channelTitle', 'channelId',  'title', 'description', 'duration_timedelta', 'duration_hhmmss', 'tags', 
               'viewCount', 'likeCount', 'favoriteCount','commentCount',
               'videoCountMonth', 'videoCountYear']]


    return df_




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
    
    




    



