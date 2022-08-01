package org.spark.belludi;
import org.apache.spark.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;


public class Main {

    public static void wordCounter(String fileName){
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        String Version = sparkContext.version();
        String fileName1 = "E:\\Developer\\BigData\\TrendyTech\\Week9\\search_data.txt";
        JavaRDD<String> inputFile = sparkContext.textFile(fileName1);
        JavaPairRDD<String, Integer> wordCount = inputFile.flatMap(content ->
                Arrays.asList(content.split(" "))
                .iterator())
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((x,y) -> x+y )
                .mapToPair(word -> new Tuple2<Integer,String>(word._2(),word._1()))
                .sortByKey()
                .mapToPair(word -> new Tuple2<String,Integer>(word._2(),word._1()));
        wordCount.foreach(data -> {
            System.out.println(data._1()+ " - "+data._2());
        });

        System.out.println("Java Spark version "+Version);
    }

    public static void main(String[] args) {
//        if(args.length == 0 ){
//            System.out.println("No file");
//        }
        wordCounter("a");
        System.out.println("Hello world!");
    }

}