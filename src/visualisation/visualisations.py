# visualistions.py
"""Module contains functions that visualise the YouTube data retrieved. 

Functions:

    `prep_df_for_visualisation(df)`: 
        Prepares the dataframe for easy visualisation (groups columns etc.)

    `viz_video_counts(df, chart_title)`:
        Bar charts the number of videos per month.

    `viz_line_chart(df, metric, chart_title)`:
        Returns a line graph with the given metric over time.

"""

import pandas as pd
import plotly.express as px
import numpy as np


def prep_df_for_visualisation(df):
    """Groups the dataframe so it is structured in a way so that it can be passed
    to the visualisation functions for quick and easy visualisation.

    Parameters
    ----------
    df : pandas dataframe
        A pandas dataframe that contains the following columns:
            publishedAtYearMonth, publishedAtYear, publishedAtMonth, videoCountMonth, 
            videoCountYear, viewCount, likeCount, commentCount.

    Returns
    -------
    df_grouped : pandas dataframe
        The input dataframe grouped and ready for visualisation.

    Notes
    ------
    
    
    """
    # group the views, likes and comment numbers by year and month
    df_group = (
        df.groupby(by=[df['publishedAtYearMonth'], df['publishedAtYear'], df['publishedAtMonth'], 
                       df['videoCountMonth'], df['videoCountYear']])[
        ['viewCount', 'likeCount', 'commentCount']
        ]
        .sum()
        .astype(int)
        .sort_values(["publishedAtYear", "publishedAtMonth"], ascending=True)
        )
    
    df_group.reset_index(inplace=True)
    
    # change column order
    df_group = df_group[['publishedAtYearMonth', 'publishedAtYear', 'publishedAtMonth',
                 'viewCount', 'likeCount', 'commentCount',
                 'videoCountMonth', 'videoCountYear']]

    return df_group




def viz_line_chart(df, metric, chart_title):
    """Returns a line chart given a dataframe and metric.

    Parameters
    -----------
    df : pandas dataframe
        A grouped dataframe created by the `prep_df_for_visualisation` function.

    metric : string
        Name of the metric we want to visualise.

    chart_title : string
        Title for the graph.

    Returns
    -------
    fig : plotly line chart
        A line chart created with plotly.

    Notes
    ------    
    
    """

    fig = px.line(df, 
              x=df["publishedAtYearMonth"], 
              y=f"{metric}",
              # color="publishedAtYear",
              # line_group="publishedAtYear",
              title=f"{chart_title}",
              line_shape='spline',
              labels={"publishedAtYearMonth": "",
                      "viewCount": "Views", 
                      "likeCount": "Likes", 
                      "commentCount": "Comments",
                      # "videoCountMonth": "No. of videos in month",
                      # "videoCountYear": "No. of videos in year"
                     },
              # hover_name='publishedAtYearMonth',
              hover_data=["viewCount", "likeCount", "commentCount",
                          # "videoCountMonth", "videoCountYear" 
                         ],
              template='plotly_white',
              )
    
    
    return fig.show()



def viz_video_counts(df, chart_title):
    """Returns a bar chart displaying the number of videos released in each month.

    Parameters
    -----------
    df : pandas dataframe
        A grouped dataframe created by the `prep_df_for_visualisation` function.

    title : string
        The title of the bar chart.

    Returns
    -------
    fig : plotly line chart
        A line chart created with plotly that displays the number of videos released per month.

    Notes
    ------    
    
    """
    
    fig = px.bar(df, 
                  x=df["publishedAtYearMonth"], 
                  y=df["videoCountMonth"],
                  labels={"publishedAtYearMonth": "", 
                          "videoCountMonth": "No. of videos in month",
                          "videoCountYear": "No. of videos in year"
                         },
                 text=df_group['videoCountMonth'],
                 title=f"{chart_title}",
                  hover_data=['videoCountMonth', 'videoCountYear'],
                 template='plotly_white'
                 )
    
    
    fig.show()