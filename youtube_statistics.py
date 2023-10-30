import requests
import json
import pandas as pd
import datetime as dt

class YTstats:
    """Describe class
    
    """

    def __init__(self, api_key, channel_id):
        """Initialises a YT class

        Parameters
        ----------
        api_key (str):
            api_key obtained from google cloud project > APIs and services > credentials 
            section. 
            User will need to enable YouTube Data API v3 via Google Cloud > APIS and
            Services > Enabled APIs and services > Enable APIS AND SERVICES > YouTube Data API v3

        channel_id (str):
            User can obtain youtube channel id by:
                > navigating to the youtube channel home page 
                > Open Developer Tools in browser (F12 on Windows/Linux or option + I + J on OSX) 
                > Click on the Network tab > in the filter box type "browse" to filter the list of 
                network requests 
                > Click on any interactive element of the channel page, e.g. the Home 
                or Videos tab 
                > In the right side panel you should see a network request to the /browse 
                endpoint 
                > Click on it and the Channel ID will appear in the Request Payload under the 
                browseId parameter

        """

        self.api_key = api_key
        self.channel_id = channel_id
        self.channel_title = None
        self.channel_statistics = None
        self.video_data = None

    
    def get_channel_statistics(self):
        """Retrieves basic overview statistics of the specified channel.
        
        """

        # define url we want to make a request to - we specify the channel id, api key and the resource property
        # that the response will include (statistics); see https://developers.google.com/youtube/v3/docs/channels/list for more info
        url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&id={self.channel_id}&key={self.api_key}"
        print(url)
        
        # send get request to store the information
        json_url = requests.get(url)
        # print(json_url)

        # convert the data into a python object
        data = json.loads(json_url.text)
        print(data)

        # store only the channel stats elements of the respons
        try:
            channel_stats_data = data["items"][0]["statistics"]

        except:
            print("Error occurred... add a better exception handling block to get useful information "
                  "about the error")
            channel_stats_data = None

        # test for locating channel title
        channel_title = data["items"][0]["snippet"]["title"].replace(" ", "_").lower()
        # store channel title in self dictionary
        self.channel_title = channel_title
        print(f"Channel title: {channel_title}")

        # add the channel title (which comes from the 'snippet' resource) to the channel 
        # statistics dictionary
        channel_stats_data['title'] = self.channel_title

        # store the channel statistics in the self.channel_statistics objct
        self.channel_statistics = channel_stats_data

        return data
    



    def get_channel_video_data(self):
        """
        
        
        """

        # 1) get video ids
        channel_videos = self._get_channel_videos(limit=50)
        print(channel_videos)
        print(len(channel_videos))



        # 2) get video statistics




    def _get_channel_videos(self, limit=None):
        """This will call the `_get_channel_videos_per_page()` function which will extract the video ids from
        the json response.
        
        
        """

        # set the url
        url = f"https://www.googleapis.com/youtube/v3/search?key={self.api_key}&channelId={self.channel_id}&part=id&order=date"

        # check limit value and data type, change to string if necessary
        if limit is not None and isinstance(limit, int):
            url += "&maxResults=" + str(limit) 

        # print(url)

        # This function calls the `_get_channel_videos_per_page` function which returns the data for each video on the page, and the next page token - 
        # this will be used to essentially start the process of getting all the video ids as it will grab the data from the first page of the api response 
        # and we'll then enter a while loop to loop through the rest of the pages. 
        # NOTE: the 'videos' variable will be a dictionary as this is what the function `_get_channel_videos_per_page()` returns
        videos, next_token = self._get_channel_videos_per_page(url)

        # implement safety measure to avoid endless api calls
        idx = 0
        # use a while loop to loop through the pages and return the video details 
        while(next_token is not None and idx < 20):
            next_url = url + "&pageToken=" + str(next_token)
            next_videos, next_token =  self._get_channel_videos_per_page(url=next_url)
            videos.update(next_videos) # use built in dictionary methood 'update' to append the next_videos dictionary
                                       # to the videos dictionary (I think we'll end up with a dictionary of dictionaries)
            # add 1 to the index as part of the safety check to avoid an endless loop
            idx += 1

        return videos




    def _get_channel_videos_per_page(self, url):
        """
        
        """

        json_url = requests.get(url)
        print(json_url)
        data = json.loads(json_url.text) # text gives us the raw text)
        channel_videos = {}

        # we need to locate the items key so if it's not there we return nothing
        if "items" not in data:
            return channel_videos, None
        
        # the json response is a dictionary and one of the keys is 'items', the value for the 
        # items key is a list of dictionaries; we want to access the 'id' value, which itself
        # is a dictionary, one of whom's keys is 'videoId' and it is the value of this key that 
        # we ned to access. 
        # NOTE: the other key in the 'id' dictionary is 'kind' - the majority of
        # values for kind will be 'youtube#video' (which is fine), but some will be youtube#playlist
        # and we don't want the playlist Ids

        # store the 'items' key in a variable (this variable is now a list, and each item in the
        # list is a dictionary)
        item_data = data["items"]

        # a key in the json response is 'nextPageToken' which we need the value of so we can loop
        # through all the pages in the response and get all the video Ids
        next_page_token = data.get("nextPageToken", None)

        # loop through the items in the json response
        for item in item_data:
            try:
                # get the kind and store in a variable (youtube#video, youtube# playlist etc.)
                kind = item["id"]["kind"]
                
                # grab the video Ids
                if kind == "youtube#video":
                    video_id = item["id"]["videoId"]
                    # store the data in the channel_videos dictionary, using the video id as the key and 
                    channel_videos[video_id] = {}

                elif kind == "youtube#playlist":
                    playlist_id = item["id"]["playlistId"]
                    channel_videos[playlist_id] = {}

            except KeyError as ke:
                print(ke)

        return channel_videos, next_page_token


    def dump(self):
        """Dump the data into a json file.
        
        """
        if self.channel_statistics == None:
            return
        
        # create a timestamp for file name
        time = dt.datetime.now().strftime("%Y-%m-%d--%H:%M:%S")
         
        # create name of the json file we'll save
        file_name = f"{self.channel_title}_{time}.json"
        
        with open(file_name, 'w') as f:
            json.dump(self.channel_statistics, f, indent=4) # indent is optional formatting parameter

        print(f"File dumped: {file_name}")


