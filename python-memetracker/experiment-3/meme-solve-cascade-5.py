#!/usr/bin/env python
from __future__ import print_function
import csv
import numpy as np
import cvxpy as CVX
from collections import defaultdict
import simplejson as json

nodes = []
nodeHash = {}
edges = {}

with open('nodes.txt','r') as nodesFile:
    for node in nodesFile:
        rownode = node.replace('\n','').split(',')
        nodes.append(rownode[1])
        nodeHash[rownode[0]] = rownode[1]

num_nodes = len(nodes)
# we have scale the timescale into 0 to 1.0
# while 1 is equal to 30 days for this memetracker problem
time_period = 1.0

cascades = defaultdict(lambda : [])

# getting cascades for every nodes will consume so much memory
# instead of reading all the cascade we will read the cascade in
# the prediction loop. The cascade file is ordered by the node id
# anyway so it will save time and memory

"""
# get cascade id for every nodes:
nodeCascades = {}
nodeCascadesHash = {}
with open('cascade-file.txt','r') as cascadesFile:
    # Reading cascades
    for cascadesRow in cascadesFile:
        #print(cascadesRow)
        casjson = json.loads(cascadesRow)
        cascade_id = casjson['casid']
        for cascade in casjson['cas']:            
            dst        = int(cascade['node']-1)
            at         = float(cascade['timescale'])
            # keys = 'cascade_id', 'dst', 'at'
            assert at <= time_period, "Infection after observation period."
            cascades[cascade_id].append((at, dst))
            # append cascadeid to nodes
            if dst not in nodeCascades.keys():
                nodeCascades[dst] = []
                nodeCascadesHash[dst] = {}
            if cascade_id not in nodeCascadesHash[dst].keys():
                nodeCascadesHash[dst][cascade_id] = {}
                nodeCascades[dst].append(cascade_id)
#print(json.dumps(nodeCascades))
# Sort according to time
for cascade_id in cascades.keys():
    cascades[cascade_id] = sorted(cascades[cascade_id])
"""
casFile = open('cascade-file-observation.txt','r')

# Possible edges: if we have never seen any infection travel down an edge, the
# best estimate for alpha for that edge is 0, i.e. it does not exist.
# The possible edges is already maintained in edges.txt file
# so we can easily load the edges from the file
possible_edges = set()
# edges hash, infer following matrix
edges = {}
with open('edges.txt','r') as edgesFile:
    for edge in edgesFile:
        edgeJson = edge.replace('\n','').split(',')
        possible_edges.add((edgeJson[0],edgeJson[1]))
        if edgeJson[0] not in edges.keys():
            edges[edgeJson[0]] = []
        edges[edgeJson[0]].append(edgeJson[1])

# Formulating the problem for each row of influence matrix A
#A = np.zeros((num_nodes, num_nodes), dtype=float)
probs = []
results = []

def logSurvival(t_i, t_j, alpha_ji):
    # survival function for the transmission
    #print('log alpha: {}'.format(alpha_ji))
    return -alpha_ji * (t_i - t_j)

def hazard(t_i, t_j, alpha_ji):
    # hazard function when one node succesfully infected
    #print('haz alpha: {}'.format(alpha_ji))
    #return alpha_ji
    return alpha_ji

# Convex problem
# Start definition of the problem
observationnode = -1
for target_node in range(num_nodes):
    print(target_node)
    print(observationnode)
    if observationnode < target_node:
        # convert nodes to observation matrix to minimize computation
        # in convex problem
        convexNodesCount = 0
        convexNodes = {}
        convexNodesArr = []
        # read cascades from file
        cascades = {}
        while observationnode < target_node:
            obsNode = json.loads(casFile.readline())
            print(obsNode)
            observationnode = obsNode['node']-1
        for obsCascades in obsNode['cascades']:
            cascade_id = obsCascades['casid']
            cascades[cascade_id] = []
            for obsCas in obsCascades['cas']:
                dst = int(obsCas['node'])-1
                # add to convexNodes
                if dst not in convexNodes.keys():
                    convexNodes[dst] = convexNodesCount
                    convexNodesArr.append(dst)
                    convexNodesCount+=1
                at = float(obsCas['timescale'])
                # keys = 'cascade_id', 'dst', 'at'
                # it's ok for now since we already fix our time observation window
                # in the cascade creation we can assume that cascade time is
                # following the observation time rules
                # assert at <= time_period, "Infection after observation period."
                cascades[cascade_id].append((at, dst))
        # recount the nodes by using the keys length
        num_nodes = len(convexNodes.keys())
    # continue the loop increasing the target node because this node is not in our interest        
    if observationnode > target_node:
        continue

    print(cascades)

    # This is one column of the alpha matrix
    Ai = CVX.Variable(num_nodes, name='A[:, {}]'.format(target_node))

    # Which edges are constrained to be zero
    constraints = []
    # we are interested in inferring the transmission rate
    """
    for j in range(num_nodes):
        if (j, target_node) not in possible_edges:
            constraints.append(Ai[j] == 0)
        else:
            constraints.append(Ai[j] >= 0)
            #constraints.append(Ai[j] >> 0)
    """
    for j in range(num_nodes):
        if j == convexNodes[target_node]:
            constraints.append(Ai[j] == 0)
        else:
            constraints.append(Ai[j] >= 0)

    #print(constraints)

    # Constructing the log-likelihood for the target_node
    expr = 0
    myCount = 0

    # this line is used if we are interested in the neighborhood
    # cascade as well, but to do this we need to load all the cascades
    """
    # nothing in cascades related to this node, just skip this value
    if target_node not in nodeCascades.keys():
        continue
    #myCascades = [cascades[x] for x in nodeCascades[target_node]]    
    observationCascades = nodeCascades[target_node].copy()
    # check neighborhood cascade
    if target_node in edges:
        for neighbor in edges[target_node]:
            if neighbor in nodeCascades:
                print('got something from {}'.format(neighbor))
                [observationCascades.append(x) for x in nodeCascades[neighbor]]
    print(observationCascades)
    # uniqfy the observationCascades
    observationCascades = list(set(observationCascades))
    """

    for c_idx, c in cascades.items():
    #for nodecas in nodeCascades[target_node]:
    #for nodecas in observationCascades:
        #c_idx = nodecas
        #c = cascades[c_idx]
        #print(myCount)
        ##print(c)
        myCount+=1
        infection_time_arr = [x[0] for x in c if x[1] == target_node]

        #print('cascade: {}'.format(c))
        assert len(infection_time_arr) <= 1

        # computing the survival function of
        # the node i given cascade from j
        # this is unlikely to happen except if we are interested in
        # relation with other cascades on the edges
        if len(infection_time_arr) == 0:
            # Node 'i' was not infected in this cascade
            for j in range(len(c)):
                alpha_ji = Ai[convexNodes[c[j][1]]]
                #print('alpha_ji surv: {}'.format(alpha_ji))
                t_j = c[j][0]
                T = time_period
                #print('log sur1: {}'.format(logSurvival(T, t_j, alpha_ji)))
                expr += logSurvival(T, t_j, alpha_ji)
                #print('log expr: {}\n'.format(expr))
        else:
            # Node 'i' was infected in this cascade
            infection_time = infection_time_arr[0]
            t_i = infection_time

            if c[0][0] != infection_time:
                # build the logSurvival and hazard function for
                # the infection
                # as a result this function will build an expression
                # that will be solved by convex problem            
                log_sum = 0
                for j in range(len(c)):
                    t_j = c[j][0]
                    alpha_ji = Ai[convexNodes[c[j][1]]]
                    #print('alpha_ji haz: {}'.format(alpha_ji))

                    if t_j < t_i:
                        expr+=logSurvival(t_i,t_j,alpha_ji)
                        #print('log sur2: {}'.format(logSurvival(t_i,t_j,alpha_ji)))
                        log_sum+=hazard(t_i,t_j,alpha_ji)
                        #pass

                expr += CVX.log(log_sum)
                #print('haz expr: {}\n'.format(expr))
    #print('log expr: {}\n'.format(expr))
    try:
        prob = CVX.Problem(CVX.Maximize(expr), constraints)
        res = prob.solve(verbose=True,)
        probs.append(prob)
        results.append(res)
        #if prob.status in [CVX.OPTIMAL, CVX.OPTIMAL_INACCURATE]:
        tempA = np.asarray(Ai.value).squeeze().tolist()
        #print(len(tempA))
        A = {}
        for x in range(len(tempA)):            
            A[convexNodesArr[x]] = tempA[x]
        #print(A)
        #else:
        #    A[:, target_node] = -1
        #print('result: {}'.format(res))
    except BaseException as e:
        print(e)
        A = -1
    with open('prediction.txt','a') as writer:
        writer.write(json.dumps({'target_node': target_node,'res':res,'alpha': A})+'\n')
    #with open('results.txt','a') as writer:
    #    writer.write(res)



