import subprocess
import json
import os
import sys
import re
import sqlite3
import numpy as np
from datetime import datetime


sys.setrecursionlimit(100000)

conn = sqlite3.connect('meme.sqlite');
conn.text_factory = str

c = conn.cursor()

arg = sys.argv
# read twitter file
totalLine = 3
linecounter=0

# load nodes
nodes = []
nodesHash = {}
with open('nodes.txt','r') as nodesFile:
	for node in nodesFile:
		nodeArr = node.replace('\n','').split(',')
		nodes.append(nodeArr[1])
		nodesHash[nodeArr[0]] = nodeArr[1]

# load edges from 3 hops
# for this edges we load the parent as index to capture followers
edges = {}
with open('edges.txt','r') as edgesFile:
	for edge in edgesFile:
		edgeJson = edge.replace('\n','').split(',')
		if edgeJson[1] not in edges.keys():
			edges[edgeJson[1]] = []
		edges[edgeJson[1]].append(edgeJson[0])

observNodes = []
observRow = c.execute("SELECT idb from temp_observ order by idb asc")
for observ in observRow:
	observNodes.append(observ[0])


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

# fetch all node
myCount = 0
for node in observNodes:	
	# make bag for this node
	myCount+=1
	cascadeCount = 0
	cascades=[]
	# fetch the cascade from node
	cascadeRows = c.execute("SELECT urlb,casid,date,memetext from observation_cascades_2 where idb=?",[node])
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
		urlVocabs = c2.execute("SELECT a.domain,a.date,a.url from clusterurl a where a.url=? order by date asc",[cascadeRow[0]])
		vocabRow = urlVocabs.fetchone();
		#print('vocab: {}'.format(vocabRow))
		startDate = datetime.strptime(vocabRow[1],'%Y-%m-%d %H:%M:%S').timestamp()
		myArr.append({'node': nodesHash[vocabRow[0]],'time': 0,'date': vocabRow[1],'text': ''})
		i+=1
		# second cursor to trace next cascade
		c1 = conn.cursor()
		childCascades = c1.execute('SELECT distinct ida, date,memetext from observation_cascades_2 a where urlb=? order by date asc',[cascadeRow[0]])
		# fetch child cascade
		for cascade in childCascades:
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
					myArr.append({'node': cascade[0],'time': cascadeTime,'date': cascade[1],'text': cascade[2]})
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
	with open('cascade-file-parent.txt','a') as casfile:
		casfile.write(json.dumps({'node':node,'cascades':cascades})+'\n')