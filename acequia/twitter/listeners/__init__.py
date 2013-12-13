# Load different backends
try:
	from .tweepy import TweepyDummyStreamListener, TweepyWriterStreamListener
except ImportError:
	print("WARNING: Tweepy not found. Tweepy-based listeners won't be loaded")

try:
	from .twython import TwythonDummyStreamListener
except ImportError:
	print("WARNING: Twython not found. Twython-based listeners won't be loaded")