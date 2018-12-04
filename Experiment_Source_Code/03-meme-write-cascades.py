import subprocess
import json
import os
import sys
import re
import sqlite3
import numpy as np
from datetime import datetime

sys.setrecursionlimit(100000)

arg = sys.argv
# read twitter file
totalLine = 3
linecounter=0

conn = sqlite3.connect('meme.sqlite');
conn.text_factory = str

c = conn.cursor()

try:
	c.execute("CREATE TABLE observation_nodes (id integer,node text)")
except BaseException as e:
	print(e)
	# read url

# create index for clusterurl and cascades for fast processing
try:
	c.execute("CREATE INDEX observationnodes_node ON observation_nodes(node)")
	c.execute("CREATE INDEX clusternew_cid on cluster_new(cid)")
	c.execute("CREATE INDEX clusterurlnew_url ON clusterurl_new(url)")
	c.execute("CREATE INDEX cascades_urlb ON cascades(urlb)")
except BaseException as e:
	print(e)


def insertObservationNodes(node):
	params = []
	params.append(node['id'])
	params.append(node['node'])
	c = conn.cursor()
	c.execute("INSERT INTO observation_nodes VALUES (?,?)",params)


# insert observation nodes to extract the cascades that came from this
# observation domain, you can
# change this impelementation depend on what nodes condition
# you want to observe
# you can also directly inject data to observation node table in sqlite
# Drop the observation_nodes table first if you want to rerun this
# script
print('Insert Observation Nodes')
rowsCas = c.execute("SELECT distinct domaina from cascades union select distinct domainb from cascades")
j = 0
for row in rowsCas:
	j+=1
	insertObservationNodes({'id': j,'node': row[0]})
conn.commit()

print('make nodes file with nodes id')
# load url vocabulary
urlVocabs = c.execute("SELECT b.id,a.date,a.url from clusterurl_new a,observation_nodes b where b.node=a.domain")
vocabs = {}
for urlvocab in urlVocabs:
	if urlvocab[2] not in vocabs:
		vocabs[urlvocab[2]] = {'node': urlvocab[0],'date':urlvocab[1]}
	else:
		if urlvocab[1] < vocabs[urlvocab[2]]['date']:
			vocabs[urlvocab[2]] = {'node': urlvocab[0],'date':urlvocab[1]}	


# load nodes id
with open('nodes.txt','w') as nodesFile:
	pass

nodesId = {}
nodeVocab = c.execute("SELECT id, node from observation_nodes")
for myNode in nodeVocab:
	if myNode[1] not in nodesId:
		nodesId[myNode[1]] = myNode[0]
		with open('nodes.txt','a') as nodesFile:
			nodesFile.write('{},{}\n'.format(myNode[1],myNode[0]))

edgesfile = open('edges.txt','w')
edgesfile.close()
edges = {}
edgesfile = open('edges.txt','a')

print('extracting cascades')
with open('cascade-file-parent.txt','w'):
	pass

# extract the cascade
observNodes = []
observRow = c.execute("SELECT id,node from observation_nodes order by id asc")
for observ in observRow:
	observNodes.append(observ[1])

# we will write cascades based on three hopes nodes to minimize the workload on
# predicting transmission rates
# because we interested in the predicting transmission between edges
# we follow the cascade as
# - first, select cascade from child = x
# - get the same cascade from other child nodes that are neighbor of x and have time
#	span mode than x
# - we use 30 days as the time observation window
# - put it in cascade bag
meanTime = 2505600
arrTime = []

# write cascades
# fetch all node
myCount = 0
for node in observNodes:	
	# make bag for this node
	myCount+=1
	cascadeCount = 0
	cascades=[]
	# fetch the cascade from node
	cascadeRows = c.execute("SELECT urlb,date,memetext from cascades where domainb=?",[node])
	idb = nodesId[node]

	print('{},{}'.format(myCount,node))
	# fetch all cascade
	for cascadeRow in cascadeRows:
		myArr = []
		# to track recurrence, don't look back
		domainHist = []
		i = 0
		length = 0
		#print('{},{},{}'.format(cascadeRow[0],node,cascadeRow[1]))
		c2 = conn.cursor()
		# get startdate from vocab url
		urlVocabs = c2.execute("SELECT a.domain,a.date,a.url from clusterurl_new a where a.url=? order by date asc",[cascadeRow[0]])
		vocabRow = urlVocabs.fetchone();
		#print('vocab: {}'.format(vocabRow))
		startDate = datetime.strptime(vocabRow[1],'%Y-%m-%d %H:%M:%S').timestamp()
		myArr.append({'node': nodesId[vocabRow[0]],'time': 0,'date': vocabRow[1],'text': ''})
		i+=1
		# second cursor to trace next cascade
		c1 = conn.cursor()
		childCascades = c1.execute('SELECT distinct domaina, date , memetext,urla from cascades a where urlb=? order by date asc',[cascadeRow[0]])

		# fetch child cascade
		for cascade in childCascades:
			ida = nodesId[cascade[0]]
			if ida not in edges:
				edges[ida] = []
				if idb not in edges[ida]:
					edgesfile.write('{},{}\n'.format(ida,idb))
					edges[ida].append(idb)					

			# lines read
			myDate = datetime.strptime(cascade[1],'%Y-%m-%d %H:%M:%S').timestamp()
			if startDate < myDate:
				cascadeTime = myDate - startDate
				# break early if the time reach its meantime
				if cascadeTime > meanTime:	
					break
				# check reccurence
				# we don't handle reccurence right now
				# so if there is a reccurence simply skip the cascade
				if cascade[0] not in domainHist:
					myArr.append({'node': ida,'time': cascadeTime,'date': cascade[1],'text': cascade[2]})
					#print('{} {} {}'.format(cascadeRow[0],i,cascadeTime))
					# tracking reccurence, don't look back
					domainHist.append(cascade[0])
					i+=1

		#finishTrace = True
		if i>1 :
			#if maxTime < myArr[len(myArr)-1]['time']:
			#	maxTime = myArr[len(myArr)-1]['time']
			#meanTime+=myArr[len(myArr)-1]['time']
			arrTime.append(myArr[len(myArr)-1]['time'])
			cascades.append({'casid': cascadeCount,'url': cascadeRow[0],'cas': myArr,'cascount': i})

	for i in range(cascadeCount):
		for mycas in cascades[i]['cas']:
			# divide by meanTime
			mycas['timescale'] = mycas['time'] / meanTime
	if len(cascades)>0:
		with open('cascade-file-parent.txt','a') as casfile:
			casfile.write(json.dumps({'node':idb,'cascades':cascades})+'\n')