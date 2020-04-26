package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

        SparkConf sparkConf = new SparkConf().setAppName("primeNumbersSum").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> intVal = sparkContext.textFile("in/prime_nums.text");
        JavaRDD<String> numbers = intVal.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());
        JavaRDD<String> validNumbers = numbers.filter(number -> !number.isEmpty());
        JavaRDD<Integer> numVal = validNumbers.map(val -> Integer.valueOf(val));
        Integer result = numVal.reduce((x, y) ->x+y );
        System.out.println(result);



    }
}
