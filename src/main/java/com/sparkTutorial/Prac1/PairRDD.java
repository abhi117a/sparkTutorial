package com.sparkTutorial.Prac1;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author a0r00rf
 */
public class PairRDD {

    public static void main (String args[]){

        List<String> inputDataDate = new ArrayList<>();
        inputDataDate.add("WARN: Tuesday 4 Sepetember 0405");
        inputDataDate.add("ERROR: Tuesday 4 Sepetember 0408");
        inputDataDate.add("FATAL: Wednesday 5 Sepetember 1632");
        inputDataDate.add("ERROR: Friday 7 Sepetember 1845");
        inputDataDate.add("WARN: Saturday 8 Sepetember 1942");

        SparkConf conf = new SparkConf().setAppName("startSpark1").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);


        javaSparkContext.parallelize(inputDataDate).mapToPair(rawDat -> new Tuple2<>(rawDat.split(":")[0],1L))
                .reduceByKey((a,b)->a+b).foreach(tuple->System.out.println(tuple._2));


        //GroupBy
        javaSparkContext.parallelize(inputDataDate).mapToPair(rawDat -> new Tuple2<>(rawDat.split(":")[0],1L)).groupByKey()
                .foreach(tuple-> System.out.println(tuple._1 + " "+ Iterables.size(tuple._2) ));

    }



}
