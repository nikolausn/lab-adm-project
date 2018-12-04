import json
import numpy as np
import math, random, copy
import numpy as np
import sys
import pandas as pd


tedges = {}
tfollowes = {}
cascade_checker = {}
cascade_count=0
cascades = {}
nodeCascades = {}
with open('cascade-file-parent-probs.txt','r') as casFile:
    for casRead in casFile:
        # prepare cascade
        # load cascade from the file
        obsNode = json.loads(casRead)
        #print(obsNode)
        parent_node = obsNode['node']
        if parent_node not in tfollowes:
            tfollowes[parent_node] = []
        for obsCascades in obsNode['cascades']:
            if obsCascades['url'] not in cascade_checker:
                cascade_checker[obsCascades['url']] = 1
            else:
                continue
            cascade_id = cascade_count
            cascades[cascade_id] = {'probs': obsCascades['probs'],'cas': []}
            for obsCas in obsCascades['cas']:
                dst = int(obsCas['node'])
                if dst not in tfollowes[parent_node]:
                    tfollowes[parent_node].append(dst)
                if dst not in tedges:
                    tedges[dst] = []
                if parent_node not in tedges[dst]:
                    tedges[dst].append(parent_node)
                
                at = float(obsCas['time'])/2505600
                cascades[cascade_id]['cas'].append((at, dst))                

                # append cascadeid to nodes
                if dst not in nodeCascades.keys():
                    nodeCascades[dst] = []
                nodeCascades[dst].append(cascade_id)
            # add new cascade id
            cascade_count+=1

neighbor_net = {}
for key, parents in tedges.items():
    neighborArr = []
    for parent in parents:
        if parent in tfollowes:
            for neighbor in tfollowes[parent]:
                if neighbor in parents:
                    if neighbor not in neighborArr:
                        neighborArr.append(neighbor)
    if len(neighborArr) > 0:
        neighbor_net[key] = neighborArr     

with open('neighbor_net.txt','w') as neighFile:
    neighFile.write(json.dumps(neighbor_net))


baseAcc = pd.read_csv('prediction-child-parents-stat.txt')

# get sample node list, 10 for each node
nodelist = baseAcc['TotalNode'].unique()
nodelist = np.sort(nodelist)
#sampleArr = []
for totalnode in nodelist:
    total_sample=200
    #targetnodes = baseAcc[baseAcc['TotalNode']==totalnode]['TargetNode']
    targetnodes = baseAcc[baseAcc['TotalNode']==totalnode]
    targetnodes = targetnodes[targetnodes['TargetNode'].isin(array_aha)].TargetNode
    total_sample=total_sample if targetnodes.shape[0] > total_sample else targetnodes.shape[0]
    sample = targetnodes.iloc[random.sample(range(targetnodes.shape[0]),total_sample)].values.tolist()
    if len(sample)>0:
#        print(sample[0])
#        print(neighbor_net[sample[0]])
    #print()
        with open('sample-neighbor-experiment.txt','a') as sampleFile:
            sampleFile.write('{}\n'.format(json.dumps({'totalnode':int(totalnode),'sample':sample})))
    #sampleArr.append(sample)    