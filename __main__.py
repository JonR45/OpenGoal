from src.processing.get_youtube_data import YouTubeStats

# api key from google cloud youtube-channel-analysis project apis and services credentials
API_KEY = "AIzaSyASfwtKSfUAAGR6DUM0dhmx48AfbZo1MzY"

# open goal channel id
channel_id = "UCArk93C2pbOvkv6jWz-3kAg"

# specify date range to avoid maxing out the YouTube API request quota
published_after = "2022-06-01T00:00:00Z"  # Format: 1970-01-01T00:00:00Z
published_before = "2023-01-01T00:00:00Z"  # Format: 1970-01-01T00:00:00Z

# initialise class for obtaining video data from specified date range
yt_stats = YouTubeStats(api_key=API_KEY, channel_id=channel_id,
                        published_after=published_after, published_before=published_before)

# get the basic channel statistics
yt_stats.get_channel_statistics()

# get data for each video between the dates specified at start of program
yt_stats.get_videos_data()

# output the data to JSON format
yt_stats.export_to_json()
