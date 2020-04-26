package com.sparkTutorial.Prac1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author a0r00rf
 */
public class Mapp {

    public static void main (String args[]){

        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(45);
        inputData.add(55);
        inputData.add(15);


        List<String> inputDataDate = new ArrayList<>();
        inputDataDate.add("WARN: Tuesday 4 Sepetember 0405");
        inputDataDate.add("ERROR: Tuesday 4 Sepetember 0408");
        inputDataDate.add("FATAL: Wednesday 5 Sepetember 1632");
        inputDataDate.add("ERROR: Friday 7 Sepetember 1845");
        inputDataDate.add("WARN: Saturday 8 Sepetember 1942");


        SparkConf conf = new SparkConf().setAppName("startSpark").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd =javaSparkContext.parallelize(inputData);



        JavaRDD<String> orginalLogRdd = javaSparkContext.parallelize(inputDataDate);



        //Reduce Exercise
        Integer result = rdd.reduce((a,b)-> a+b);

        //Map Exercise
        JavaRDD<Double> sqrtRdd= rdd.map(val -> Math.sqrt(val));

        //get Count Values
        Long countVAl = sqrtRdd.count();

        Integer countOfElements = sqrtRdd.map(a-> 1).reduce((a,b)->a+b);

        //Map-Tuple-CustomObject conversion
        JavaRDD<IntegerWithSquareRootMap> res = rdd.map(a-> new IntegerWithSquareRootMap(a));

        //How to use Tuple in Java
        Tuple2<Integer,Double> valu = new Tuple2<>(1,2.0);

        //Above method using tuple instead of using custom class
        //Map-Tuple-CustomObject conversion
        JavaRDD<Tuple2<Integer,Double>> resTuple = rdd.map(a-> new Tuple2<>(a,Math.sqrt(a)));

        //UsingPairRDDS

        JavaPairRDD<String,String> javaPairRDD = orginalLogRdd.mapToPair(values->{

           String[] cols =  values.split(":");
           String level = cols[0];
           String date = cols[1];
           return new Tuple2<>(level,date);
        });

        JavaPairRDD<String, Iterable<String>> gropByKeys = javaPairRDD.groupByKey();

        JavaPairRDD<String, Long> javaPairRDD1 = orginalLogRdd.mapToPair(value -> {
            String[] cols = value.split(":");
            return new Tuple2<>(cols[0],1L);
        });


        JavaPairRDD<String,Long> reduceByKeyVal = javaPairRDD1.reduceByKey((a,b)-> a+b);
        reduceByKeyVal.foreach(stringLongTuple2 ->  System.out.println(stringLongTuple2._2));

        javaSparkContext.close();


    }



}
