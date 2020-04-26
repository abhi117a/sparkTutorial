package com.sparkTutorial.rdd.nasaApacheWebLogs;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.codehaus.janino.Java;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */


        SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]");;
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> data07 = sparkContext.textFile("in/nasa_19950701.tsv");
        JavaRDD<String> data08 = sparkContext.textFile("in/nasa_19950801.tsv");

        JavaRDD<String> onlyHost07 = data07.map(line-> line.split("\t")[0]);
        JavaRDD<String> onlyHost08 = data08.map(line->line.split("\t")[0]);

        JavaRDD<String> result = onlyHost07.intersection(onlyHost08).filter(host-> !host.equals("host"));

        result.saveAsTextFile("out/nasa_logs_same_hosts.csv");







    }


}
