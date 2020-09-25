# Scalable Recommendation of Wikipedia Articles to Editors Using Representation Learning 

![](https://github.com/digitalTranshumant/WikiRecNet-ComplexRec2020/blob/master/WikiRecDescription.png)

In this repository we share the code to compute the Wikipedia Articles embeddings describing [on this paper](...).

The full code is divided in two repositories. In this reposotory we describe the procedure to build the Wikipedia Graph, that is used as training data, and in this [other repository](https://github.com/pyalex/GraphSAGE) (a modification of [GraphSAGE](http://snap.stanford.edu/graphsage/)) we share the code to compute the article's embeddings. 

## Paper Abstract

Wikipedia is edited by volunteer editors around the world. Considering the large amount of existing content (eg over 5M articles in English Wikipedia), deciding what to edit next can be difficult, both for experienced users that usually have a huge backlog of articles to prioritize, as well as for newcomers who that might need guidance in selecting the next article to contribute. Therefore, helping editors to find relevant articles should improve their performance and help in the retention of new editors. In this paper, we address the problem of recommending relevant articles to editors. To do this, we develop a scalable system on top of Graph Convolutional Networks and Doc2Vec, learning how to represent Wikipedia articles and deliver personalized recommendations for editors. We test our model on editors' histories, predicting their most recent edits based on their prior edits. We outperform competitive implicit-feedback collaborative-filtering methods such as WMRF based on ALS, as well as a traditional IR-method such as content-based filtering based on BM25. All of the data used on this paper is publicly available, including graph embeddings for Wikipedia articles, and we release our code to support replication of our experiments. Moreover, we contribute with a scalable implementation of a state-of-art graph embedding algorithm as current ones cannot efficiently handle the sheer size of the Wikipedia graph.

[Download the full paper from arxiv.](https://arxiv.org/abs/2009.11771)

## Citation

If you find this repository useful for your research, please consider citing our paper: 
```
@inproceedings{WikiRecNet,
    author = {Moskalenko,Oleksii and Parra, Denis and Saez-Trumper, Diego},
    title = {Scalable Recommendation of Wikipedia Articles to Editors Using Representation Learning},
    year = {2020},
    booktitle = {Workshop on Recommendation in Complex Scenarios at the ACM RecSys Conference on Recommender Systems (RecSys 2020)},
    keywords = {Wikipedia, RecSys,  Graph Convolutional Neural Network, Representation Learning},
    location = {Virtual Event, Brazil},
}

```




## Wikipedia Dump Processing

This is collection of bash/python scripts and spark jobs aimed to parse SQL and XML dumps from Wikipedia [https://dumps.wikimedia.org/]
to prepare dataset for training GraphSAGE [https://github.com/pyalex/GraphSAGE] model on the task of 
Representation Learning for Wikipedia Articles. 

### 0. Setup

List of required dumps:
```
{lang}wiki-{date}-page.sql.gz
{lang}wiki-{date}-redirect.sql.gz
{lang}wiki-{date}-pagelinks.sql.gz

# For User Edition History 
{lang}wiki-{date}-stub-meta-history[1-9]*.xml.gz

# For article categories
enwiki-{date}-categorylinks.sql.gz

# Latest WikiData for cross-lingual connections
https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2
```

We assume that you already downloaded all required dumps (use [https://dumps.wikimedia.org/enwiki/20181220/] for example)


List of required installations
```
open-jvm
sbt
python2.7
pip
jq
lbzip2
gcloud (if you will run spark jobs in Google Dataproc)
```

Python requirements
```
pip install -r python-requirements.txt
```

In addition, you will need to install graph-tool [https://git.skewed.de/count0/graph-tool/wikis/installation-instructions]
 if you want to store graph in binary compact format (for fast saving/loading).


### 1. List of jobs
 
 **edu.ucu.wikidump.ArticleGraph** - takes as input next tables: pages, pagelinks, redirects, categories (optional).
 This job resolves all pagelinks to actual page ids (in Wikipedia all links are by title). In the meantime all links to 
 redirect pages are being replaced to actual pages. In addition, if `categories` table is provided - 
 WikiProject categories are being extracted as second output
 
 **edu.ucu.wikidump.CrossLingualMapping** - conversion from WikiData objects (where all known translation of the same articles
 are gathered as one object) into pairs pageId <-> pageId for two specified languages (requires running scripts/read-wikidata.sh first)
 
 **edu.ucu.wikidump.Revision** - takes as input history of articles revisions, filter out bots and minor activities and
 returns edited articles grouped by user (requires running scripts/parse-revisions.py first)
 
 **edut.ucu.graph.GraphCleaner** - for cleaning article graph (generated by **edu.ucu.wikidump.ArticleGraph**)
 
### 2. Usage Examples (with Dataproc)

First, let's build all scala code
```
sbt package
```

Now, we can create dataproc cluster

```
gcloud dataproc clusters create \
    --project YOUR_PROJECT YOUR_CLUSTER_NAME --zone us-central1-a \
    --worker-machine-type n1-highmem-32 \
    --num-workers 2
```

We also need to put all files to Google Storage, so they will be available to our Dataproc cluster

```
gsutil cp *.sql gs://some-bucket/enwiki/
```

We assume, that you already unpacked SQL dumps. *GZipped files cannot be parallelized by Spark.*
Now we can start with building edges for graph

```
gcloud dataproc jobs submit spark --cluster YOUR_CLUSTER_NAME --jars target/scala/wiki2graph-2.11-0.1.jar \
    --class edu.ucu.wikidump.ArticleGraph -- \
    --pagelinks gs://some-bucket/enwiki/enwiki-20181220-pagelinks.sql
    --pages gs://some-bucket/enwiki/enwiki-20181220-pages.sql
    --redirects gs://some-bucket/enwiki/enwiki-20181220-redirects.sql
    --output gs://some-bucket/enwiki/article-graph-edges/
    
```
___

For creating mapping between articles from different Wikipedia localizations (eg. English and Ukrainian)
we need to have
1. wikidata json file (you may keep it compressed, since it would take more than 500Gb of disk to unpack it)
2. pages.sql for both languages

```
# We decompressing wikidata and extracting only required fields
# That saves a lot of disk space

export WIKIDATA=wikidata-20181112-all.json.bz2
scripts/read-wikidata.sh


gsutil wikidata-flatten.json gs://some-bucket/

gcloud dataproc jobs submit spark --cluster YOUR_CLUSTER_NAME --jars target/scala/wiki2graph-2.11-0.1.jar \
    --class edu.ucu.wikidump.CrossLingualMapping -- \
    --from enwiki \
    --to ukwiki \
    --from-pages gs://some-bucket/enwiki/enwiki-20181220-pages.sql
    --to-pages gs://some-bucket/ukwiki/ukwiki-20181220-pages.sql
    --wikidata gs://some-bucket/wikidata-flatten.json
    --output gs://some-bucket/en-uk-mapping/
```
___


Creating user history of editions:
1. Download all stub-meta-history dumps (~35Gb for English)
2. Unpack all XML archives into one tsv (keeps only required fields, saves space) - *can take some time*
```python scripts/parse-revisions enwiki-20181120-stub-meta-history[1-9]*.xml.gz en-revisions.tsv```
3. Run re-grouping job
```
gsutil en-revisions.tsv gs://some-bucket/enwiki/

gcloud dataproc jobs submit spark --cluster YOUR_CLUSTER_NAME --jars target/scala/wiki2graph-2.11-0.1.jar \
    --class edu.ucu.wikidump.Revision -- \
    --revisions gs://some-bucket/enwiki/en-revisions.tsv \
    --min-date 2015-01-01 \
    --min-bytes 100 \
    --output gs://some-bucket/user-editions/
```


## Building the embeddings

Go to this [other repository](https://github.com/pyalex/GraphSAGE).
