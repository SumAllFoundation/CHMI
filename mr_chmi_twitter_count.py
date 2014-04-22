#!/usr/local/bin/python

import json
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, JSONProtocol
from shapely.geometry import * 
# from dateutil import parser
from geopy.geocoders import OpenMapQuest as OMQ
from geopy.geocoders import GoogleV3
import csv

#1 ) Dump 
#   Tweets: mongoexport --db CHMI --collection tweets --file ~/Dropbox/code/CHMI/data/tweets.json --host ec2-184-73-72-229.compute-1.amazonaws.com
#2 ) Run
#   python ~/Dropbox/code/CHMI/mr_chmi_twitter_count.py ~/Dropbox/full_tweets.json  > ~/Dropbox/code/CHMI/data/output0407.json --file ~/Dropbox/code/CHMI/data/us-counties.json --file ~/Dropbox/code/CHMI/data/search_short.csv --file ~/Dropbox/code/utilities/dbparams.json -r emr


class MongoKeywordCount(MRJob):
    #Job to count the number of tweets per keyword, date, location. 

    #Input is a JSON file. 
    INPUT_PROTOCOL = JSONValueProtocol
    #OUTPUT_PROTOCOL = JSONProtocol

    #mapper_init ~ __init__
    #Assing properties
    def mapper_init(self):
        #State Boundaries
        with open('us-counties.json') as f: states = json.loads(f.read())
        self.state_polygons = [(shape(state['geometry']),state['id']) for state in states['features'] ]

        #Keywords
        keywords = []
        with open('search_short.csv','rU') as f:
            lines = csv.reader(f)
            #Skip Header
            lines.next()
            for line in lines:
                keywords.append(line[0])
            
        self.keywords = keywords

        #GeoMappers
        keys=[]
        with open('dbparams.json') as f:
            for line in f:
                keys.append(json.loads(line))
            keys = keys[0]
        self.omq = OMQ(keys['geo']['OMQ'])
        self.gg = GoogleV3()




    #No key defined because of INPUT_PROTOCOL
    def mapper(self, _, tweet):

        #Datetime
        # dt = parser.parse(tweet['created_at'])
        # dt = dt.strftime('%Y/%m')

        result = None
        keyword = None
        match = False
        combined_key = None

        #Tweet 
        text = tweet['text'].encode('utf8')
        #print text

        #Keyword Match
        for k in self.keywords:
            #String Match
            if k in text:
                keyword = k
                break

        #Location
        user_loc = tweet['user']['location'].encode('utf8')

        #If tweet coordinate is embedded in the tweet
        if tweet['coordinates']:
            try:
                point = shape(tweet['coordinates'])
                match = True
            except:
                #print 'Shapely error'
                pass
            #If tweet coordinate not available. 
            #Grab User Location if lonlat information not available 
            #This is an approximation but should still work. 
        elif user_loc:
            #print 'Geocode user location: ', user_loc
            try:        
                result =  self.omq.geocode(user_loc)
            except:
                try:
                    #print 'error in open mapquest. try google api'
                    result = self.gg.geocode(user_loc)
                except:
                    #print 'google api not responding. skipping the entry'
                    pass
            #Convert into GeoJSON File if there sis a match
            if result:
                #print result
                coordinate = {'coordinates': [result[1][1],result[1][0]], 'type': 'Point'}
                point = shape(coordinate)
                #print point.xy
                match = True


        #Match State and Keyword
        if match and keyword:
            #Match State
            try:
                for i, state in enumerate(self.state_polygons):
                    if state[0].contains(point):
                        #print 'Found polygon: {}'.format(state[1])
                        #print 'key :', k, dt, state[1]
                        #Increment the Counter
                        combined_key = (k, state[1])
                        #Counter
                        self.increment_counter('mapper', 'processed tweets', 1)
                        #Break State Loop
                        break
            except:
                pass
        else:
            self.increment_counter('mapper', 'skipped tweets', 1)

        #Generator
        yield combined_key, 1


    def reducer(self, combined_key, values):

        yield combined_key, sum(values)

if __name__ == '__main__':
    MongoKeywordCount.run()



