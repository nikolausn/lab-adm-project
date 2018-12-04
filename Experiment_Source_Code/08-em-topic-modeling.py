import json
import numpy as np
import matplotlib.pyplot as plt
import math, random, copy
import numpy as np
import sys
import matplotlib.pyplot as plt
import matplotlib as mpl
from scipy.misc import logsumexp
import pickle

#x_mat = np.matrix(np.tile(np.random.randint(1000),(50,50)))
totalClust=10
#?np.zeros

x_mat = np.zeros((20001,31494))
with open('feat-obs.txt','r') as docfile:
    # skip 3 lines
    for i in range(3):
        next(docfile)
    for doc in docfile:        
        content = doc.split(',')
        x_mat[int(content[0])-1,int(content[1])-1] = int(content[2])
    
x_mat.shape

vocabulary = []
with open('words-obs.txt','r') as vocfile:    
    for voc in vocfile:
        voc = voc.replace('\n','')
        vocabulary.append(voc)
npvoc = np.array(vocabulary)


totalClust=15


#np.asarray(p_jk[1,]/p_jk[1,].sum())
#np.apply_along_axis(lambda x: x,p_jk,0)

#?np.random.randint

# calculate cluster center using kmeans for initialization
centroids, distortion = kmeans(x_mat,k_or_guess=totalClust, iter=5)
#normalizes ps ie mus
mus = np.zeros((totalClust, centroids.shape[1]))
for j in range(totalClust):
    mus[j] = (centroids[j]+.0001)/(np.sum(centroids[j])+centroids.shape[1]/10000)

# total clusters
j=totalClust
# total documents
i=x_mat.shape[0]
# total vocabularies
k=x_mat.shape[1]

# initialize pi_j matrix with same probability
pi_j = np.repeat(1/j,j)

# prepare words probability for each clusters and documents
p_jk = np.matrix(np.tile(1/k,(j,k)))
# prepare w_ij matrix initialize
#w_ij = np.matrix(np.tile(0,(i,j)))
w_ij = np.zeros((i,j))

# initialize using kmeans
p_jk = mus

# small values to avoid log(0)
epsilon = 1e-7

p_jk = p_jk + epsilon
# normalize p_jk so the probability will be equal one
#p_jk = [p_jk[x,]/sum(p_jk[x,]) for x in range(j)]
p_jk = np.asarray([np.asarray(p_jk[x,]/p_jk[x,].sum()) for x in range(j)])
print(p_jk.shape)
p_jk = np.reshape(p_jk,(j,k))
# test normalize function
p_jk[1,].sum()

from scipy.misc import logsumexp

trace_Q = 100
old_Q = 0
treshold = 1e-4
while(trace_Q > treshold):
    sum_Qpart = 0
    print('ESTEP')
    for iTrace in range(i):
        w_ij_coll = []
        for jTrace in range(j):
            w_ij_ex = x_mat[iTrace,].dot(np.log(p_jk[jTrace,])) + np.log(pi_j[jTrace])
            sum_Qpart = sum_Qpart + w_ij_ex
            w_ij_coll.append(w_ij_ex)
        # normalize w_ij
        log_vec = np.array(w_ij_coll)
        max_w_ij = max(w_ij_coll)
        log_vec = log_vec - max_w_ij
        les = logsumexp(log_vec)
        log_vec = log_vec - les
        
        w = np.exp(log_vec)
        w_ij[iTrace,] = w/np.sum(w)
        
    #print(w_ij)
    
    print('MSTEP')
    for jTrace in range(j):
        print(jTrace)
        sigmaXiWij = np.zeros(k)
        sigmaWij = 0
        sigmaXi1wij = 0
        for iTrace in range(i):
            #sigmaXiWij = sigmaXiWij + x_mat[iTrace,] * np.exp(w_ij[iTrace,jTrace])
            sigmaXiWij = sigmaXiWij + x_mat[iTrace,] * w_ij[iTrace,jTrace]
            #sigmaXi1wij = sigmaXi1wij + (x_mat[iTrace,].dot(np.ones(k))) * np.exp(w_ij[iTrace,jTrace])
            sigmaXi1wij = sigmaXi1wij + (x_mat[iTrace,].dot(np.ones(k))) * w_ij[iTrace,jTrace]
            #sigmaWij = sigmaWij + np.exp(w_ij[iTrace,jTrace])
            sigmaWij = sigmaWij + w_ij[iTrace,jTrace]
        p_jk[jTrace,] = sigmaXiWij / sigmaXi1wij
        pi_j[jTrace] = sigmaWij / k
    
    pi_j = pi_j / sum(pi_j)
    
    # normalize
    p_jk = p_jk + epsilon
    # normalize p_jk so the probability will be equal one
    #p_jk = [p_jk[x,]/sum(p_jk[x,]) for x in range(j)]
    p_jk = np.asarray([np.asarray(p_jk[x,]/p_jk[x,].sum()) for x in range(j)])
    #print(p_jk.shape)
    p_jk = np.reshape(p_jk,(j,k))
    # test normalize function
    #p_jk[1,].sum()
    
    #calculate the Q change
    new_Q = (sum_Qpart*(w_ij)).sum()
    #print(new_Q)
    trace_Q = abs((old_Q-new_Q)/new_Q)
    #trace_Q = abs(old_Q-new_Q)
    old_Q = new_Q
    #print(trace_Q.shape)
    print(trace_Q)

em_results = {}
em_results['pi_j'] = pi_j
em_results['p_init'] = mus
em_results['p_jk'] = p_jk
em_results['w_ij'] = w_ij
em_results['cluster'] = totalClust
pickle.dump( em_results, open( "em_results_15b.p", "wb" ) )