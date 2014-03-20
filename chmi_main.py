




import urllib2
from bs4 import BeautifulSoup as bs
from geopy.geocoders import OpenMapQuest as OMQ
from geopy.geocoders import GoogleV3
import pandas as pd
import numpy as np
import json


omq = OMQ(dbparams['geo']['OMQ'])
gg = GoogleV3()

url = 'http://www.testprepreview.com/college_contactinfo.htm'
htmldoc = urllib2.urlopen(url).read()
#Paragraphs on HTML doc
soup = bs(htmldoc)
ps = soup.findAll('p')


#GeoCode University Locations
locs =[]
for p in ps:
	try:
	    #print 'error in open mapquest. try google api'
	    result = gg.geocode(p)
	    print result
	except:
		print p
		#print 'google api not responding. skipping the entry'
		pass
	locs.append(result)

def none_filter(x):
	if x is not None:
		return x

locs_filtered = filter(none_filter,locs)
loc = []
for loc_filtered in locs_filtered:
	loc.append({'name': loc_filtered[0], 'lat': loc_filtered[1][0],'lon': loc_filtered[1][1]})
loc = pd.DataFrame(loc)


#Match colleges with Counties
with open('us-counties.json') as f: counties = json.loads(f.read())
polygons = [(shape(state['geometry']),state['id']) for state in counties['features'] ]
filtered =[]
for k, loc_ in loc.iterrows():
	coordinate = {'coordinates': [loc_['lon'],loc_['lat']], 'type': 'Point'}
	point = shape(coordinate)
	try:
		for i, state in enumerate(polygons):
		    if state[0].contains(point):
		    	loc_['_id'] = state[1]
		    	filtered.append(loc_)
	except:
		c += 1 	   
college_locations = pd.DataFrame(filtered) 	





###Map/Reduce Job
###Explanotry Analysis, Clean up and Merge the Data
#Load the MR Output
out=[]
with open('data/output0320.json', 'r') as results_file:
	for line in results_file:
	    try:
	        line = line.split('\t')
	        keys = line[0].replace(']','').replace('[','')
	        keys = keys.split(',')
	        #Remove " 
	        keys[0] = keys[0].replace('"','')
	        out.append({'k1': str(keys[0]),'k2':str(keys[1]),'v' : int(line[1])})
	    except:
	        print 'skip ', line


	keywords = pd.DataFrame(out)
	keywords.columns = ['keyword','county','count']
	keywords['keyword'] = keywords['keyword'].map(lambda k : k.strip())
	keywords['county'] = keywords['county'].map(lambda k : k.strip())
	keywords['keyword'] = keywords['keyword'].map(lambda k : k.strip())
	#Merge Adderall related keyword
	keywords['keyword'] = keywords['keyword'].map(lambda k:  'adderall' if k == 'aderall' else k)
	keywords['keyword'] = keywords['keyword'].map(lambda k:  'adderall' if k == 'adderal' else k)
	#Getting rid of the keywords
	keywords = keywords[keywords['keyword'] != 'oc']
	#Bucket into major categories. Pull in search key terms and categories
	categories = pd.read_csv('data/search.csv')
	categories.index = categories.keyword
	categories = categories.drop('keyword',axis=1)
	##Index
	keywords.index = keywords.keyword
	keywords = keywords.drop('keyword',axis=1)
	##Merge
	keywords = keywords.join(categories).reset_index()
	#County Level Groupping  -  Categories. The data is used to plot the D3 animation. 
	category_totals_by_county = keywords.groupby(['county','category']).sum().reset_index()
	category_totals_by_county = category_totals_by_county.pivot(columns='category', index='county', values='count')
	category_totals_by_county[pd.isnull(category_totals_by_county)]=0
	#Normalize 

	#AskGeo Data. 
	with open('us-counties.json') as f: counties = json.loads(f.read())
	l_ = []
	for county in counties['features']:
		a = np.array(county['geometry']['coordinates'])
		if county['geometry']['type'] in 'Polygon':
			try:
				mean_  = a[0].mean(axis=0)
			except:
				mean_ = np.array(a[0]).mean(axis=0)
		else:
			mean_ = np.array(a[0][0]).mean(axis=0)
		print {'location':  mean_, 'county': county['id']}
		l_.append({'location':  mean_, 'county': county['id']})


	s_ = l_[:10]
	county=[]
	for i,v in enumerate(l_):
		try:
			str2 = '%2C'.join([str(v['location'][1]),str(v['location'][0])])
			str1 = "http://api.askgeo.com/v1/759/59d515e8e0e35186b88f6a2b83810820de54f59b01dcfcf0c07bb9b59ecdbb43/query.json?points=%s&databases=UsCounty2010" % (str2)
			a = urllib2.urlopen(str1)
			z = json.loads(a.read())
			z = z['data'][0]['UsCounty2010']
			y = {'population' :z[u'CensusTotalPopulation'], 'county': z[u'CensusGeoCode'].strip()}
			county.append(y)
		except:
			print str2 
	#Dictionary
	pop =pd.DataFrame(county)
	pop.index = pop['county']
	pop = pop.drop('county',axis=1)
	merged = category_totals_by_county.join(pop)

	#Fill population NA's with means
	merged = merged.apply(lambda x: x.fillna(x.mean()),axis=0)
	#Normalize
	merged = merged.div(merged.population, axis='index') * 1000
	category_totals_normalized = merged.drop('population',axis=1)
	#Format + Save to File
	pd.options.display.float_format = '{:20,.2f}'.format
	category_totals_normalized.to_csv('data/chmi_county_data.csv')



#CartoDB Export
features_new=[]
with open('us-counties.json') as f: counties = json.loads(f.read())
features = counties['features']
for feature in features:
	try:
		feature[u'properties']['id'] = feature['id']
		features_new.append(feature)
	except:
		pass
counties['features'] = features_new
#DUmp
with open('us-counties-cartodb.json','w') as f: f.write(json.dumps(counties))

