![Common Crawl Logo](https://avatars.githubusercontent.com/u/1194841?s=64)

# Common Crawl WARC Examples

This repository contains both wrappers for processing WARC files in Hadoop MapReduce jobs and also Hadoop examples to get you started.

There are three examples for Hadoop processing:

+ [WARC files] HTML tag frequency counter using raw HTTP responses
+ [WAT files] Server response analysis using response metadata
+ [WET files] Classic word count example using extracted text

For development, you likely want to start with input files stored locally in the `data/` subdirectory. To acquire the files, you can use any HTTP client or (if you are on AWS) the [AWS CLI](https://aws.amazon.com/cli/).

    mkdir data
    cd data/
    wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2013-48/segments/1386163035819/warc/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.gz
    wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.wet.gz

or on AWS

    mkdir data
    aws s3 cp s3://commoncrawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/warc/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.gz data/
    aws s3 cp s3://commoncrawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.wet.gz data/

To build and run in [Hadoop local or non-distributed mode](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation):

    mvn package
    <path-to-hadoop>/bin/hadoop jar target/cc-warc-examples-0.5-SNAPSHOT-jar-with-dependencies.jar org.commoncrawl.examples.mapreduce.WETWordCount -Dmapreduce.framework.name=local file:/tmp/cc/wet-word-count file:$PWD/data/*.wet.gz
    
Note: all three examples require that you specify the output directory and all input files or directories.
      
# License

MIT License, as per `LICENSE`
