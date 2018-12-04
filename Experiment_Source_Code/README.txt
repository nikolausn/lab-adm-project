--------------------------------------------------------------------------------

Data Preprocessing

1. Download memetracker cluster dataset contains meme texts and their page related from http://snap.stanford.edu/memetracker/srcdata/clust-qt08080902w3mfq5.txt.gz 

2. Run memetracker memes ingestor script '01-meme-cluster-winfo.py' . This script will produce sqlite database with memes data on it

3. Download memetracker hyperlink dataset contains pages and their additional information such as hyperlink in the webpage and quotes from http://snap.stanford.edu/data/memetracker9.html . For example: http://snap.stanford.edu/data/bigdata/memetracker9/quotes_2008-0clust-qt08080902w3mfq5.txt.txt.gz.

4. Run the meme ingestor script '02-meme-ingestor.py' to ingest the textfile, nodes and cascades into the sqlite database
Change the file name to be ingested according to what do you want to observe in the open filename code below
....
counter = 0

with open('quotes_2008-08.txt','r') as memes:
	comments = []
	links = []
	page = ''
	time = ''
...

5. Extract the observation cascade into text files including the nodes and edges using '03-meme-write-cascades.py' . This script will produce 3 files, nodes.txt, edges.txt, and cascade-file-parent.txt , which will be use for our experiment

--------------------------------------------------------------------------------

Direct Network Experiment
NETRATE BASELINE:
1. Run the script '04-netrate-baseline-direct-network.py' . This script will compute the transmission rate for all the cascade on the observation files. The output is 'prediction-child-parents.txt'


NETRATE IMPROVEMENT WITH TOPIC INFERENCE:
1. Extract the word from cascade database using '05-meme-extract-text.py' this will produce three files 'cas_id.txt', ' feature.txt', and 'ex-feature.txt' which contains all the terms that are related to all the cascades. This script will take a long time to finish since it will extract all the words and count the frequency for its cascade. The script will also try to filter stop words.
2. For simplicity we will not use all the data there so we filter out the terms by ingesting the data into sqlite database running '06-meme-extract-text-db.py'
3. And then extract our observation terms in cascade that has less than 250 words using '07-meme-extract-text-obs.py'  This will produce 3 files as well 'fet-obs.txt', ' word-obs.txt', and 'docs-obs.txt'
This files will be used for the EM Topic Modeling 
4. Run the EM Topic modeling script '08-em-topic-modeling.py', this will produce pickle file 'em_results_15b.p' contains probability of p_jk (probability of word j on topic)
5. After get the pickle file, we ingest the topic probability into the cascade using '09-meme-merge-tprob.py' , this will produce new observation cascades file with probability 'cascade-file-parent-probs.txt'
6. Now we can run the NETRATE plus TOPIC inference implementation using
This will produce file 'prediction-child-parents-p.txt' as the transmission rate prediction with 15 different topics
7. Compute the accuracy using '11-meme-compute-acc.py', this script will produce two files: 'prediction-child-parents-stat.txt' for the NETRATE baseline, and 'prediction-child-parents-p-stat.txt' for the accuracy on the NETRATE IMPROVEMENT.

--------------------------------------------------------------------------------

Neighborhood Prediction Experiment
DATA PREPROCESSING:
1. Extract Neighbor: Make sure you run the compute accuracy first '11-meme-compute-acc.py', because we will use the 'prediction-child-parents-stat.txt' file.  To produce file to extract nodes that has our neighborhood condition for the Neighborhood Prediction experiment purposes, run '12-extract-neighbor.py' . This script will write all possible nodes and its possible neighbor using the condition mentioned in the final report. The extracted file is 'neighbor_net.txt'. This script will also extract neighbor sample for many variances of total nodes in 'sample-neighbor-experiment.txt'.

2. Run the NETRATE baseline script for neighbor prediction using '13-netrate-baseline-neighbor-prediction.py' and NETRATE IMPROVEMENT script using '14-netrate-improvement-neighbor-prediction.py' . Make sure to run the baseline first, because we will use the random neighbor produced by the baseline script for direct comparisson. These scripts will produce two files, 'prediction-neighbors-experiment.txt', and 'prediction-neighbors-probs-exp.txt', respectively.

3. Compute the accuracy of our neighborhood experiment using '15-meme-compute-acc-neigh.py' . This script will produce two files: 'prediction-neighbors-exp-stat.txt' for the NETRATE baseline and 'prediction-neighbors-probs-exp-stat.txt' for the NETRATE IMPROVEMENT

--------------------------------------------------------------------------------

Neighbor Prediction all data:
This is the implementation part of our neighborhood experiment, to produce all posible neighbor among the sample data, for this purpose we just provide the NETRATE improvement implementation only. This script will draw random neighbor from the observation nodes and try to give neighbor recommendation if it produced positive transmission rate. Run the script '16-netrate-improvement-neighbor-implementation.py' this will produce 'prediction-neighbors-probs.txt' which contains the transmission rate for all observation nodes.

--------------------------------------------------------------------------------

Visualization:
For the visualization we provide a Jupyter Notebook in '17-netrate-visualization.ipynb'
Make sure you finished all the experiment above because the visualization will use all the data produced from the experiment.

--------------------------------------------------------------------------------

If you have any queries, please contact Nikolaus Nova Parulian (nnp2@illinois.edu)


Thanks and Enjoy.