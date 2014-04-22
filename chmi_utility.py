#!/usr/local/bin/python
import pandas as pd

'Taking the mr input json, performing some clean-up, and normalizing by county population based on Census 2010.\
The output is used on D3 visualization'

INPUT = 'mr_output.json'
INPUT_COUNTY = 'county_population.csv'
OUTPUT = 'output_normalized.json'


def chmi_utility(input=INPUT,count=INPUT_COUNTY,output=OUTPUT):

	###Map/Reduce Job
	###Explanotry Analysis, Clean up and Merge the Data
	#Load the MR Output
	out=[]
	with open(input, 'r') as results_file:
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
	#Index convert to int
	ix = map((lambda x: int(x)), category_totals_by_county.index)
	category_totals_by_county.index = ix
	#Dictionary
	pop = pd.read_csv(count)
	pop.index = pop['county']
	pop = pop.drop('county',axis=1)
	#Anything > 10 is included to avoid small set bias problem. 
	category_totals_by_county_ab = category_totals_by_county[category_totals_by_county.sum(axis=1) > 60 ]
	merged = category_totals_by_county_ab.join(pop)
	merged = merged.apply(lambda x: x.fillna(x.mean()),axis=0)
	merged = merged.div(merged.population, axis='index') * 10000
	category_totals_normalized = merged.drop('population',axis=1)
	category_totals_normalized.to_csv(output)
	print 'Normalized output saved to file'

if __name__ == '__main__':
	chmi_utility()