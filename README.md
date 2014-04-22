Prescription Drug Abuse Dashboard Repository



Map Reduce Job aggregates the twitter data on a state and category basis.

D3 visualization creates an interactive map to monitor Twitter activity

A prototype can be found here:

http://chmi-monitor.s3-website-us-east-1.amazonaws.com/

Here is a brief description of the methodology  we used. Twitter data is available upon request. 


1)	Collect keyword-related Tweet data. 

Use the Tweepy library to retrieve and store tweets in MongoDB. 

	https://github.com/dzorlu/utilities

	$ python tweepy_mongodb_update.py --dbparams 'dbparams.json' --inputlist 'twitter_keywords.json'

2)	A data dump of all tweets to a local file.

	$ mongoexport --db CHMI --collection tweets --file tweets.json --host ec2-xxx.compute-1.amazonaws.com

3)	A MapReduce Job to aggregate tweet counts by desired geography - county in this case. 'Search_short' is a subset of 'twitter_keywords.json' 

	https://github.com/SumAllFoundation/CHMI/tree/master/mr

	$ python mr_chmi_twitter_count.py tweets.json  > output.json --file us-counties.json --file search_short.csv --file dbparams.json -r emr

Only 1 percent of the tweets approximately has location information. To enrich the sparse dataset we also use the user location information if available. 

More information on MrJOB module can be found here:

	https://pythonhosted.org/mrjob/

4)	Format the data and normalize by county population for visualization.

We only take the counties that have more than 30 tweets in total to avoid large varitions within small sample sets. We then normalize by county population. A good and relevant discussion can be found here:

	http://press.princeton.edu/chapters/s8863.pdf 

Please refer to the code for the method.

	https://github.com/SumAllFoundation/CHMI/blob/master/chmi_utility.py

5)	The code to create the visualization can be found here:

	https://github.com/SumAllFoundation/CHMI/blob/master/d3/chmi_choropleth.html 


