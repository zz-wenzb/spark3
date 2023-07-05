package com.wenzb.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    f5()
  }

  def f5(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //保存分区文件
    rdd.saveAsTextFile("output")
    sc.stop()
  }

  def f4(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    //配置默认分区数
    sparkConf.set("spark.default.parallelism","5");
    val sc = new SparkContext(sparkConf)
    //第二个参数是分区数,不传递默认
    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //默认八个分区（当前环境默认的最大核数）
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //保存分区文件
    rdd.saveAsTextFile("output")
    sc.stop()
  }

  def f3(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)
    //以文件为单位读取数据
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)
    sc.stop()
  }

  def f2(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)
    //可以是文件，也可以是目录，也可以是通配符，还可以是分布式存储路径（hdfs）
    //    val rdd: RDD[String] = sc.textFile("datas/1.txt")
    val rdd: RDD[String] = sc.textFile("datas")
    rdd.collect().foreach(println)
    sc.stop()
  }

  def f1(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD");
    val sc = new SparkContext(sparkConf)
    val seq = Seq[Int](1, 2, 3, 4)
    //并行
    //    val rdd: RDD[Int] = sc.parallelize(seq)
    //底层调用parallelize
    val rdd = sc.makeRDD(seq)
    rdd.collect().foreach(println)
    sc.stop()
  }

}
