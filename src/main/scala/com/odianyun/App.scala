package com.odianyun

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\hadoop-common-2.2.0-bin-master")
//    val conf = new SparkConf().setAppName("sparkApp").setMaster("local")
    val conf = new SparkConf().setAppName("sparkApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val wordRdd = sc.textFile("hdfs://heimdall01.test.hadoop.com:8020/user/spark/README.md")
    val countRdd = wordRdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    countRdd.foreach(tuple=>println(tuple))
//    countRdd.saveAsTextFile("hdfs://heimdall01.test.hadoop.com:8020/user/spark/wordCountOutPut")
  }
}
