package com.wenzb.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

object DataLoad {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("load")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    f5(spark)
    spark.stop()
  }

  def f5(spark: SparkSession): Unit = {
    import spark.implicits._
    val rdd: RDD[User2] = spark.sparkContext.makeRDD(List(User2("lisi", 20), User2("zs", 30)))
    val ds: Dataset[User2] = rdd.toDS
    //方式 1：通用的方式 format 指定写出类型
//    ds.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/spark")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "user")
//      .mode(SaveMode.Append)
//      .save()
    //方式 2：通过 jdbc 方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark", "user", props)
  }

  def f4(spark: SparkSession): Unit = {
    import spark.implicits._
    //方式 1：通用的 load 方法读取
    //    spark.read.format("jdbc")
    //      .option("url", "jdbc:mysql://localhost:3306/spark")
    //      .option("driver", "com.mysql.jdbc.Driver")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      .option("dbtable", "user")
    //      .load().show

    //方式 2:通用的 load 方法读取 参数另一种形式
    //    spark.read.format("jdbc")
    //      .options(
    //        Map(
    //          "url" -> "jdbc:mysql://localhost:3306/spark?user=root&password=123456",
    //          "dbtable" -> "user",
    //          "driver" -> "com.mysql.jdbc.Driver"
    //        )
    //      )
    //      .load().show
    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/spark", "user", props)
    df.show
  }

  //user_visit_action.csv
  def f3(spark: SparkSession): Unit = {
    spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("input/user_visit_action.csv")
      .show()
  }

  //Spark 读取的 JSON 文件不是传统的 JSON 文件，每一行都应该是一个 JSON 串
  def f2(spark: SparkSession): Unit = {
    import spark.implicits._
    val path = "input/test1.json"
    val peopleDF: DataFrame = spark.read.json(path)
    peopleDF.createOrReplaceTempView("people")
    val teenagerNamesDF: DataFrame = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
  }

  def f1(spark: SparkSession): Unit = {
    spark.sql("select * from json.`input/user.json`").show
  }
}
