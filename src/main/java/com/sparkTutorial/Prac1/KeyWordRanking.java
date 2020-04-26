package com.sparkTutorial.Prac1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author a0r00rf
 */
public class KeyWordRanking {



    public static void main (String args[]){
        List<String> inputDataDate = new ArrayList<>();
        inputDataDate.add("WARN: Tuesday 4 Sepetember 0405");
        inputDataDate.add("ERROR: Tuesday 4 Sepetember 0408");
        inputDataDate.add("FATAL: Wednesday 5 Sepetember 1632");
        inputDataDate.add("ERROR: Friday 7 Sepetember 1845");
        inputDataDate.add("WARN: Saturday 8 Sepetember 1942");

        SparkConf conf = new SparkConf().setAppName("startSpark").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        JavaRDD<String> rawData = javaSparkContext.parallelize(inputDataDate);
        JavaRDD<String> noNewLines = rawData.filter(line -> line.trim().length()>0);
        JavaRDD<String> noNonCharacters = noNewLines.map(line -> (line.replaceAll("[^a-zA-Z\\s]","")));
        JavaRDD<String> flatMapped = noNonCharacters.flatMap(line -> Arrays.asList(line.split(" ")).iterator());





    }

}
