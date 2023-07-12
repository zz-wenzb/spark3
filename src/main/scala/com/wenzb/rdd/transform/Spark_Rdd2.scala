package com.wenzb.rdd.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_Rdd2 {
  def main(args: Array[String]): Unit = {
    f3
  }

  def f3(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("e", 8)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("a", 9), ("b", 5), ("c", 6), ("d", 7)))

    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    /**
     * (a,(Seq(1),Seq(4, 9)))
     * (b,(Seq(2),Seq(5)))
     * (c,(Seq(3),Seq(6)))
     * (d,(Seq(),Seq(7)))
     * (e,(Seq(8),Seq()))
     */
    rdd3.collect().foreach(println)
    sc.stop()
  }

  def f2(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("e", 8)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4),("a", 9), ("b", 5), ("c", 6), ("d", 7)))

    val rdd3: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

    /**
     * (a,(1,Some(4)))
     * (a,(1,Some(9)))
     * (b,(2,Some(5)))
     * (c,(3,Some(6)))
     * (e,(8,None))
     */
    rdd3.collect().foreach(println)
    sc.stop()
  }

  def f1(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("e", 8)))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 4),("a", 9), ("b", 5), ("c", 6), ("d", 7)))

    val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

    /**
     * (a,(1,4))
     * (a,(1,9))
     * (b,(2,5))
     * (c,(3,6))
     */
    rdd3.collect().foreach(println)
    sc.stop()
  }

}
