![Common Crawl Logo](http://commoncrawl.org/wp-content/uploads/2016/12/logocommoncrawl.png)

# Common Crawl WARC Examples

This repository contains both wrappers for processing WARC files in Hadoop MapReduce jobs and also Hadoop examples to get you started.

There are three examples for Hadoop processing:

+ [WARC files] HTML tag frequency counter using raw HTTP responses
+ [WAT files] Server response analysis using response metadata
+ [WET files] Classic word count example using extracted text

All three assume initially that the files are stored locally in the data subdirectory but can be trivially modified to pull them down from Common Crawl's Amazon S3 bucket. To acquire the files, you can use any HTTP client or (if you are on AWS) the [AWS CLI](https://aws.amazon.com/cli/).

    mkdir data
    cd data/
    wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2013-48/segments/1386163035819/warc/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.gz
    wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.wet.gz

or on AWS

    mkdir data
    aws s3 cp s3://commoncrawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/warc/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.gz data/
    aws s3 cp s3://commoncrawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wet/CC-MAIN-20131204131715-00000-ip-10-33-133-15.ec2.internal.warc.wet.gz data/

To build and run:

    mvn package
    <path-to-hadoop>/bin/hadoop jar target/cc-warc-examples-0.3-SNAPSHOT-jar-with-dependencies.jar org.commoncrawl.examples.mapreduce.WETWordCount
    
All three examples place output in the directory `/tmp/cc`.
      
# License

MIT License, as per `LICENSE`
