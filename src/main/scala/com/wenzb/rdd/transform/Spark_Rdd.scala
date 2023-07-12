package com.wenzb.rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Rdd {
  def main(args: Array[String]): Unit = {
    f12()
  }

  def f12(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6), 4)

    val value3: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(value3.collect().mkString(","))
    sc.stop()
  }

  def f11(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))

    //交集 3,4
    val value: RDD[Int] = rdd1.intersection(rdd2)
    println(value.collect().mkString(","))
    //并集 1,2,3,4,3,4,5,6
    val value1: RDD[Int] = rdd1.union(rdd2)
    println(value1.collect().mkString(","))

    //差集 1,2
    val value2: RDD[Int] = rdd1.subtract(rdd2)
    println(value2.collect().mkString(","))
    //拉链 (1,3),(2,4),(3,5),(4,6)
    val value3: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(value3.collect().mkString(","))
    sc.stop()
  }

  def f10(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3, 4))
    rdd.distinct().collect().foreach(println)

    sc.stop()
  }

  def f9(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    /**
     * 解决数据倾斜的
     * 三个参数
     * 1.抽取数据后是否将数据放回，true 放回，false 丢弃
     * 2.数据源中，每条数据被抽取的概率
     * 3.抽取数据时，随机算法的种子(不传递，默认是当前系统时间)
     */
    println(rdd.sample(
      false,
      0.4,
      1
    ).collect().mkString(","))
    sc.stop()
  }

  def f8(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val value: RDD[(Int, Iterable[Int])] = rdd.groupBy(x => x % 2)
    value.collect().foreach(println)

    sc.stop()
  }

  def f7(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val glomRdd: RDD[Int] = rdd.glom().map(_.max)
    println(glomRdd.collect().sum)
    println(glomRdd.sum())


    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    //
    //    // 【1，2】，【3，4】
    //    // 【2】，【4】
    //    // 【6】
    //    val glomRDD: RDD[Array[Int]] = rdd.glom()
    //
    //    val maxRDD: RDD[Int] = glomRDD.map(
    //      array => {
    //        array.max
    //      }
    //    )
    //    println(maxRDD.collect().sum)
    sc.stop()
  }

  def f6(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    //报错，不让这样创建
    //    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2),8, List(3, 4)))
    //    rdd.flatMap(data=>{
    //      data match {
    //        case list: List[_] => list
    //        case dat => List(dat)
    //      }
    //    }).collect().foreach(println)

    sc.stop()
  }

  def f5(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    rdd.flatMap(list => list).map(_ * 2).collect().foreach(println)

    val strRdd: RDD[String] = sc.makeRDD(List("hello spark", "hello java"))
    strRdd.flatMap(_.split(" ")).map((_, 1)).collect().foreach(println)

    sc.stop()
  }

  def f4(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else {
        Nil.iterator
      }
    }).collect().foreach(println)

    sc.stop()
  }

  def f3(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    //每个分区最大值
    rdd.mapPartitions(iter => {
      List(iter.max).iterator
    }).collect().foreach(println);

    //最大值
    println(rdd.max())

    sc.stop()
  }

  def f2(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 2)
    //按照分区处理（内存处理）
    //数据使用完不会释放，数据量比较大容易内存溢出
    rdd.mapPartitions(iter => {
      println("----------------")
      iter.map(_ * 2)
    }).collect().foreach(println);
    sc.stop()
  }

  def f1(): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator");
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8))

    //    def mapFunc(num: Int): Int = {
    //      num * 2
    //    }

    //    rdd.map(mapFunc).collect().foreach(println)
    rdd.map(_ * 2).collect().foreach(println)

    sc.stop()
  }

}
