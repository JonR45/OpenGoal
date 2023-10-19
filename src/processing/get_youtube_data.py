"""Contains class and methods for obtaining YouTube channel and video data.

Class YouTubeStats:
- `YouTubeStats(self, api_key, channel_id, published_before, published_after)`
    - Contains methods for extracting data from a YouTube channel

YouTubeStats methods:
- `get_channel_statistics(self)`
- `get_videos_data(self)`
- `get_video_ids(self, limit=None)`
- `get_video_ids_per_page(self, url)`
- `_get_single_video_data(self, video_id, part)`
- `export_to_json(self)`

"""

import requests
import os
import json
# import datetime as dt
from tqdm import tqdm


class YouTubeStats:
    """Class contains methods for extracting YouTube video data from a given channel.

        Resources
        ----------
        YouTube API:
            https://developers.google.com/youtube/v3/docs
    
    """

    def __init__(self, api_key, channel_id, published_before, published_after):
        """Initialises a YT class, passing the relevant api key and channel id.

        Parameters
        ----------
        api_key (str):
            api_key obtained from Google cloud project > APIs and services > credentials
            section. 
            User will need to enable YouTube Data API v3 via Google Cloud > APIS and
            Services > Enabled APIs and services > Enable APIS AND SERVICES > YouTube Data API v3

        channel_id (str):
            User can obtain YouTube channel id by:
                > navigating to the YouTube channel home page
                > Open Developer Tools in browser (F12 on Windows/Linux or option + I + J on OSX) 
                > Click on the Network tab > in the filter box type "browse" to filter the list of 
                network requests 
                > Click on any interactive element of the channel page, e.g. the Home 
                or Videos tab 
                > On the right side panel you should see a network request to the /browse endpoint
                > Click on it and the Channel ID will appear in the Request Payload under the 
                browseId parameter

        published_after: string
            Datetime value in a string format to specify the date range that we'll gather video data from.
                - Format: "1970-01-01T00:00:00Z"

        published_before: string
            Datetime value in a string format to specify the date range that we'll gather video data from.
                - Format: "1970-01-01T00:00:00Z"

        """

        self.api_key = api_key
        self.channel_id = channel_id
        self.published_after = published_after
        self.published_before = published_before
        self.channel_title = None
        self.channel_statistics = None
        self.video_data = None

    def get_channel_statistics(self):
        """Uses a GET request to the 'channels' resource (list method) of the YouTube API to obtain
        snippet data (channel title, channel description) and statistics data (video count,
        subscriber count, view count etc.)

        Returns
        -------
        data: 
            Python dictionary from JSON response.

        Notes
        ------
        Channels resource information:
            https://developers.google.com/youtube/v3/docs/channels/list
        """

        # define url we want to make a request to, specifying channel id, api key and the resource property
        # that the response will include (statistics)
        url = f"https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&id={self.channel_id}&" \
              f"key={self.api_key}"
        print(url)
        
        # send get request and store the response
        json_url = requests.get(url)
        # print(json_url)

        # convert the JSON response into a python dictionary
        data = json.loads(json_url.text)
        # print(data)

        # store only the channel stats elements of the response
        try:
            channel_stats_data = data["items"][0]["statistics"]

        except Exception as e:  # TODO: this is bad practice - implement better
            # exception handling and logging
            print("An unexpected error occurred: {e}")     
            channel_stats_data = None

        # get name of channel
        channel_title = data["items"][0]["snippet"]["title"].replace(" ", "_").lower()
        print(f"Channel title: {channel_title}")
        
        # store channel title in self dictionary and add it to the channel stats dictionary
        self.channel_title = channel_title
        channel_stats_data['title'] = self.channel_title

        # store the channel statistics in the self.channel_statistics object
        self.channel_statistics = channel_stats_data

        # return the python dictionary of the JSON response
        return data

    def get_videos_data(self):
        """Function performs two tasks by calling two other functions: 
            1. It calls the `get_video_ids` helper function which in turn calls the `get_video_ids_per_page`
            function, to obtain all the video ids and store them in a dictionary,

            2. It loops through the keys from the dictionary returned in step 1 (the keys will be a video id) and uses
            this to obtain data about the individual video by calling the `_get_single_video_data` helper function; it
            will then store this data in the dictionary, so we are left with a dictionary that has a video id as a key
            and a dictionary of data related to that video as the value.

        Returns
        -------
        channel_videos: dict
            Dictionary with video id as key and a dictionary of associated data as its value.
        """

        print("Getting video IDs...")
        # 1) get video ids
        channel_videos = self.get_video_ids(limit=50)
        print(f"Video IDs obtained...")
        # print(f"Length of channel videos dictionary: {len(channel_videos)}\n")
        # print(f"Channel videos dictionary (this should be a dictionary with only keys):\n{channel_videos}")

        # 2) get video data
        parts = ["snippet", "statistics", "contentDetails"]  # need data from different parts of the response; part
        # is set in the URL

        # create a loop that will gather data from each part for each video
        print(f"Getting {parts} data for every video ID...")

        # NOTE: Dictionary changing size as it was being iterated over:
        # Runtime error occurred because the dictionary was changing size as it was being iterated over and updated
        # in the for loop; handled this error by creating a copy of the dictionary and updating it in the for loop
        
        # create copy of dictionary
        # print("Creating copy of dictionary\nLooping through dictionary...\n")
        channel_videos_copy = channel_videos.copy()

        # loop through every video id we have and the specified 'parts' to obtain the data we want about each video
        for video_id in tqdm(channel_videos_copy):
            for part in parts:
                # call function to obtain the data and place th data returned into the channel video dictionary
                data = self._get_single_video_data(video_id, part)
                channel_videos[video_id].update({part: data})

        print(f"{parts} data obtained.")

        # store the dictionary of video ids (key) and their associated value (dictionary with stats about the video)
        self.video_data = channel_videos

        # return dictionary (video ids as keys and the value of each key will be a dictionary of data related to that
        # video id).
        return channel_videos

    def get_video_ids(self, limit=None):
        """Defines the URL that will be used by `get_video_ids_per_page` function and then
        calls this function to return a dictionary with the video ids as keys.

        Returns
        --------
        video_ids: Dict
            A dictionary with all the video ids as keys and an empty dictionary as the values.
        
        """

        # set the url
        url = f"https://www.googleapis.com/youtube/v3/search?key={self.api_key}&channelId={self.channel_id}&" \
              f"part=id&order=date&publishedAfter={self.published_after}&publishedBefore={self.published_before}"

        # check limit value is set (should previously have been set to 50 (safety against exceeding request quota)
        if limit is not None and isinstance(limit, int):
            url += "&maxResults=" + str(limit) 
        print(url)

        # start the process of obtaining the video ids, this will grab the video ids from page 1 and return them and
        # the next_page_token which will be used when entering the while loop below (to get the video ids from the
        # rest of the pages)
        video_ids, next_token = self.get_video_ids_per_page(url)

        # set an index tracker as part of a safety measure to avoid endless api calls
        idx = 0
        # use a while loop to loop through the pages from the JSON response URL and return the video ids
        while next_token is not None and idx < 11:
            next_url = url + "&pageToken=" + str(next_token)
            next_video_ids, next_token = self.get_video_ids_per_page(url=next_url)
            video_ids.update(next_video_ids)  # use built in dictionary method 'update' to append the
            # `next_video_ids` dictionary (which is returned from the `get_video_ids_per_page` function) to the
            # `video_ids` dictionary
            idx += 1  # add 1 to index (part of safety check to avoid endless loop leading to excessive API requests)

        # return video_ids dictionary (should be a dictionary of keys (video id) and values are empty dictionaries
        return video_ids

    def get_video_ids_per_page(self, url):
        """Uses a GET request to the YouTube API using the 'search: list' method and obtains the Ids 
        of all of the videos returned in the JSON response.

        
        The JSON response is a dictionary with nested dictionaries, we want to access the value of the
        'videoId' key which is accessed via:
            > items(key) > id(value) > videoId(key) > string_we_want_to_get(value)

        Parameters
        -----------
        url: str
            The URL string that will be used for the GET request. 
            This parameter is set in the `_get_video_ids` function.
        
        
        Returns
        --------
        channel_videos: dict
            A dictionary with `videoId` as a key and an empty dictionary as the value (the empty dictionary will be
            populated with the video stats later in the program).

        next_page_token: str
            The token (string) required for the `pageToken` parameter of the GET request url. This string is used
            for the next to access the next page of the json response results so that the next page's video
            id's can be accessed.

        Notes
        -----
        'kind' key:
            Another key in the 'id' dictionary is 'kind' - the majority of values for kind 
            will be 'youtube#video' (which is fine), but some will be youtube#playlist, and we
            don't want the playlist Ids
        
        """

        # store the JSON response in a variable; the url parameter is specified in the get_video_ids
        # helper function
        json_url = requests.get(url)
        print(json_url)
        
        # use json.loads() to load the JSON string into a python dictionary (essentially convert the JSON response
        # into a Python dictionary)
        data = json.loads(json_url.text)  # text gives us the raw text)
        # print(data)

        # create an empty dictionary; this dictionary will eventually hold the video data, with the
        # video id as a key and all the data related to that video id as its value
        channel_videos = {}

        # we need the 'items' key to exist in the response in order to access the data we want so we check
        # if it exists and return nothing if it doesn't
        if "items" not in data:
            return channel_videos, None

        # store the 'items' in a variable; each item is a dictionary and so the 'item_data' variable will be
        # a list, and each item in the list will be a dictionary
        item_data = data["items"]

        # the JSON response was set to 50 results per page, so we may have multiple pages of videos if there
        # are more than 50 videos in the response; a key in the JSON response is 'nextPageToken' which we need
        # the value of so we can access the next page of the response results to get all the video Ids
        next_page_token = data.get("nextPageToken", None)

        # it's now time to access the video ids so we loop through the 'item_data' list (remember each item in this
        # list is a dictionary) and access each video id, and save the video id as a key in our channel_videos
        # dictionary
        for item in item_data:
            try:
                # get the 'kind' ( kind could be 'youtube#video', 'youtube#playlist' etc.) and store in a variable 
                kind = item["id"]["kind"]
                
                # store the video ids if the 'kind' is a video (as opposed to a playlist or something else)
                if kind == "youtube#video":
                    video_id = item["id"]["videoId"]
                    # make the video id a key in our dictionary
                    channel_videos[video_id] = {}

                # elif kind == "youtube#playlist":
                #     playlist_id = item["id"]["playlistId"]
                #     channel_videos[playlist_id] = {}

            except KeyError as ke:
                print(ke)

        # this function will be used in a loop, so we return the channel_videos dictionary and the
        # next_page_token, so we can get to the next page of results (if there is a next page)
        return channel_videos, next_page_token

    def _get_single_video_data(self, video_id, part):
        """Retrieves data related to a single video id by making a GET request which, within the
        URL, contains the 'part=' parameter which will determine the data returned. (The 'part'
        is specified in the `_get_videos_data` function).

        Parameters
        -----------
        video_id: str
            A video id as a string which will be used in the URL as part of the GET request.
            The video id obtained using the `get_channel_video_ids` and `get_video_ids_per_page`
            functions. 

        part: str
            This is used as the value for the 'part=' parameter in the GET request URL, and will determine
            the type of data returned.
            The part(s) are determined in the `get_videos_data` function.

        Returns
        -------
        data: dict 
            Returns a dictionary of data for the specified 'part'.     
        """

        url = f"https://www.googleapis.com/youtube/v3/videos?part={part}&id={video_id}&key={self.api_key}"

        json_url = requests.get(url)
        data = json.loads(json_url.text)

        try:
            data = data["items"][0][part]
            # print(f"Data obtained for video id: {video_id}, {part}\n")
            # print(f"{data}\n")

        except Exception as e:
            print("An unexpected error occurred: {e}")
            data = {}

        return data

    def export_to_json(self):
        """Dumps the data into a .json file and saves it in the data > processed folder.
        """
        if self.channel_statistics is None or self.video_data is None:
            print("Data is none.")
            return
        
        fused_data = {self.channel_id: {"channel_statistics": self.channel_statistics, 
                                        "video_data": self.video_data}}
        
        # create a timestamp for file name
        # time = dt.datetime.now().strftime("%Y-%m-%d--%H:%M:%S")

        # reformat the published before and after variables and use for the filename
        pub_after = str(self.published_after.replace(":", "-"))
        pub_before = str(self.published_before.replace(":", "-"))

        # set the folder to save the file in
        current_working_directory = os.getcwd()
        data_processed_folder = "data/raw/JSON_response"

        # create name of the json file we'll save
        file_name = f"{self.channel_title}_vids_from_{pub_after}--{pub_before}.json"

        # join cwd, folder name and file name to set the file path
        file_path = os.path.join(current_working_directory, data_processed_folder, file_name)
        
        with open(file_path, 'w') as f:
            json.dump(fused_data, f, indent=4)  # indent is optional formatting parameter

        print(f"\n\nFile: {file_name}\nSaved: {file_path}")
