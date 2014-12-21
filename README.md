Use EMR and Spark, analyze the +/- sentiment of a 4.4 GBs of tweets stored in S3.

###Videos

Spinning up three m1.medium instances on AWS Elastic Map Reduce with a bootstrap Spark install.

<video width="640" height="400" controls>
  <source src="recordings/create-emr-spark-cluster.mp4" type="video/mp4">
  Your browser does not support the <code>video</code> element.
</video>

The results of the Spark job run on EMR. Note the presence of `sentiment.py` when I `ls` the directory contents.

<video width="640" height="400" controls>
  <source src="recordings/finished-emr-spark-job.mp4" type="video/mp4">
</video>
