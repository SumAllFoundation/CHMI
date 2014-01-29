
import json
from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol, JSONProtocol
from shapely.geometry import * 
from dateutil import parser
import pandas as pd
from geopy.geocoders import OpenMapQuest as OMQ
from geopy.geocoders import GoogleV3


#python ~/Dropbox/code/CHMI/mr_chmi_twitter_count.py ~/Dropbox/data/CHMI/subtweets.json  > out.json --file us-states.json--file search.csv --file keys.json


class MongoKeywordCount(MRJob):
    #Job to count the number of tweets per keyword, date, location. 

    #Input is a JSON file. 
    INPUT_PROTOCOL = JSONValueProtocol
    #OUTPUT_PROTOCOL = JSONProtocol

    def mapper_init(self):
        #State Boundaries
        with open('us-states.json') as f: states = json.loads(f.read())
        self.state_polygons = [(shape(state['geometry']),state['properties']['name']) for state in states['features'] ]

        #Keywords
        self.keywords = pd.read_csv('search.csv').values.ravel()
        #GeoMappers
        keys=[]
        with open('keys.json') as f:
            for line in f:
                keys.append(json.loads(line))
            keys = keys[0]
        self.omq = OMQ(keys['OMQ'])
        self.gg = GoogleV3()


    def mapper(self, _, tweet):

        #Datetime
        dt = parser.parse(tweet['created_at'])
        dt = dt.strftime('%Y/%m')

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
                        combined_key = (dt, k, state[1])
                        #Counter
                        self.increment_counter('mapper', 'processed tweets', 1)
                        #Break State Loop
                        break
            except:
                pass
        else:
            self.increment_counter('mapper', 'skipped lines', 1)

        #Generator
        yield combined_key, 1


    def reducer(self, combined_key, values):

        yield combined_key, sum(values)

if __name__ == '__main__':
    MongoKeywordCount.run()


# out=[]
# with open('part-00000', 'r') as results_file:
#     for line in results_file:
#         try:
#             line = line.split('\t')
#             keys = line[0].replace(']','').replace('[','')
#             keys = keys.split(',')
#             out.append({'k1': str(keys[0]),'k2':str(keys[1]),'k3':str(keys[2]),'v' : int(line[1])})
#         except:
            # print 'skip ', line
