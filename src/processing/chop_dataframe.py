# chop_dataframe.py
"""Module contains functions that manipulate the dataframe created after processing and cleaning
the JSON data retrieved from the YouTube API.

Functions include:

    `single_metric_dataframes(df, metrics=[]`:
        Takes a list of metrics and creates individual dataframes sorted by the given
        metric in descnding order
    
    `split_into_video_series(df, video_series_title=[])`:
        Takes a string, or list of strings, and returns a dataframe with only those strings
        in the video title.

    

"""


import pandas as pd


def single_metric_dataframes(df, metrics=[]):
    """Creates individual sorted dataframes for the given metrics. These dataframes can 
    then be used for analysis and visualisation.
    
    
    Parameters
    -----------
    df : pandas dataframe
        Dataframe that, ideally, contains the column names provided in the metrics list.
        
    metrics : list
        A list of column names.
        
        
    Returns
    ---------
    df : pandas dataframe(s)
        Returns a dataframe for each metric provided (assuming the metric is a column in
        the dataframe). The dataframe will be sorted from highest to lowest for the given
        metric.
        
    Notes
    ------
    Single metric error:
        Added logic that checked the length of the list generated during the execution of the function.
        This was because when there was only a single metric provided, a list was being returned insead of 
        the dataframe contained in the list. Explicitly transforming the single item to a dataframe with line
        
        df_sorted = pd.DataFrame(df_list[0])
        
        and  then returning the dataframe resolved this error.
    
    """
        
    # check if input is a single string, if it is then store it as a single item list
    if isinstance(metrics, str):
        metrics = [metrics]
        
    elif isinstance(metrics, list):
        metrics = [x for x in metrics]
    
    
    df_list = []
    
    
    for i, v in enumerate(metrics):
        
        v = str(v)
        
        try: 
            df_ = (df[['videoID', 'title', 
                       'publishedAt', 'publishedAtYear', v]]
                .sort_values(by=[v], ascending=False, axis=0)
                .reset_index(drop=True))

            df_list.append(df_)
            
        except KeyError as ke:
            
            print(f"""
            {ke} 
            Check that each column exists in the dataframe provided for the 'df' parameter. 
            Check the spelling of the column names provided for the 'metrics' parameter.
            Make sure the number of variables you are storing dataframes in matches the number of dataframe outputs. 
            """)
            
            pass
        
    
    # check length of dataframe list - this ensures that if only one metric is provided in the metrics parameter
    # then the dataframe is returned without error (was encountering an error when there was just a single item
    # in the list)
    if len(df_list) == 1:
        df_sorted = pd.DataFrame(df_list[0])
        return df_sorted
    
    
    elif len(df_list) > 1:
        
        return [df for df in df_list]




def split_into_video_series(df, video_series_title=[]):
    """Takes in a dataframe and the name of a YouTube video series and returns the input dataframe 
    with only the videos whose titles include the 'video_series_title' string in their title.
    
    Parameters
    ----------
    df : pandas dataframe
        Dataframe with YouTube video data.
        
    video_series_title : list
        List of strings of the title, or part of the title, of the video series we want to return a dataframe for.
        
    Returns
    -------
    df_ : dataframe
        Dataframe with only the rows whose video titles contain the specified 'video_series_title'.
        
    
    Notes
    ------
    
    
    """
    
    # check if input is a single string, if it is then store it as a single item list
    if isinstance(video_series_title, str):
        video_series_title = [video_series_title]
        
    # if input is a list then transform to lower case 
    elif isinstance(video_series_title, list):
        video_series_title = [x.lower() for x in video_series_title]
    
    
    # create empty list that will store dataframes which will be concatenated later
    df_list = []
    
    # check length of video_series_title list; if greater than 1 create multiple dataframes and then concatenate
    if len(video_series_title) > 1: 
        # loop through strings of video_series_title list
        for i, v in enumerate(video_series_title):
            
            # create dataframe with only the video titles that contain the specified string and append to list
            df_ = df.loc[df['title'].str.contains(v, case=False)] 
            df_list.append(df_)
            
        # concatenate dataframes, perform sorting and cleaning operations
        df_combined = pd.concat(df_list, ignore_index=True)
        df_combined = (df_combined
                       .drop_duplicates(subset=["videoID"])
                       .sort_values(by=['publishedAt'])
                       .reset_index(drop=True))
        
        # return the dataframe
        return df_combined
        
            
    # check length of video_series_title list; if equal to 1 then use only item in list to create the dataframe
    elif len(video_series_title) == 1: 
        # create dataframe with only the video titles that contain the specified string and append to list
        df_ = df.loc[df['title'].str.contains(video_series_title[0], case=False)]
        # perform sorting and cleaning operations
        df_ = (df_
               .drop_duplicates(subset=["videoID"])
               .sort_values(by=['publishedAt'])
               .reset_index(drop=True)
              )
           
        # return dataframe
        return df_


    
    else: # no video series names/titles provided
        print("No changes made.")
        
        return df

    
    
