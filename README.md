# WeblogChallenge
This is an interview challenge for Paytm Labs. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

# Build
Use gradle build to compile a light JAR. Meant to be deployed and run as a spark job in a Hadoop cluster.

    # built in IntelliJ IDEA
    # Outputs to 'build/libs/challenge-webLogs-spark-2.1.1-v1.0.jar'
    # To build: `./gradlew clean && ./gradlew lightJar`

# Run
This jar is called by spark using **spark-submit**
 
 Mandatory parameters : 
    **hiveDB=HiveDBNAME**   :  Hive Database Name
    **inputPath=hdfs:///user/<USERNAME>/challenge/data**  :  Input data file path

###Syntax

     ./spark-submit --master yarn-client --conf spark.ui.port=1212 --jars /Users/<USERNAME>/challenge-webLogs-spark-2.1.1-v1.0.jar \
      --driver-memory 20g --executor-memory 20g --executor-cores 4 --num-executors 10 --queue root.ABC --class org.paytm.challenge.WebLogs \
       /Users/<USERNAME>/challenge-webLogs-spark-2.1.1-v1.0.jar hiveDB=HiveDBNAME inputPath=hdfs:///user/<USERNAME>/challenge/data


## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

2. Predict the session length for a given IP

3. Predict the number of unique URL visits by a given IP

### Tools allowed (in no particular order):
- Spark (any language, but prefer Scala or Java)
- Pig
- MapReduce (Hadoop 2.x only)
- Flink
- Cascading, Cascalog, or Scalding

If you need Hadoop, we suggest 
HDP Sandbox:
http://hortonworks.com/hdp/downloads/
or 
CDH QuickStart VM:
http://www.cloudera.com/content/cloudera/en/downloads.html


### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format



## How to complete this challenge:

A. Fork this repo in github
    https://github.com/PaytmLabs/WeblogChallenge

B. Complete the processing and analytics as defined first to the best of your ability with the time provided.

C. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the Paytm Labs interview team.

D. Complete your work in your own github repo and send the results to us and/or present them during your interview.

## What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.
