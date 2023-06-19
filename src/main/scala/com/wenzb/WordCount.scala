package com.wenzb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val lines: RDD[String] = sc.textFile("datas")

//    f1(lines)
    f2(lines)

    sc.stop()
  }

  def f2(lines:RDD[String]): Unit = {
    val words: RDD[String] = lines.flatMap(line => line.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))
    //val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey((a,b) => a + b )
    val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

    val array = wordToCount.collect()
    array.foreach(println)

  }

  def f1(lines:RDD[String]): Unit = {
    val wordGroup = lines.flatMap(line => line.split(" ")).groupBy(word => word)

    val wordToCount = wordGroup.map {
      case (word, list) => (word, list.size)
    }
    val array = wordToCount.collect()
    array.foreach(println)
  }

}
