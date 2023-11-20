# data_manipulation.py
"""Module contains functions that manipulate the dataframe created after processing and cleaning
the JSON data retrieved from the YouTube API.

Functions include:

    `single_metric_dataframes(df, metrics=[]`:
        Takes a list of metrics and creates individual dataframes sorted by the given
        metric in descnding order

    

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
        the dataframe contained in the list. Explicitly transforming the single item to a dataframe and 
        then returning the dataframe resolved this error.
    
    """
    
    metrics = list(metrics)
    
    df_list = []
    
    
    for i, v in enumerate(metrics):
        
        v = str(v)
        
        try: 
            df_ = (df[['videoID', 'title', v]]
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

    

    
