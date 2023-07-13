package com.wenzb.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, TypedColumn}

object Test {

  def main(args: Array[String]): Unit = {
    //创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sql")
    //创建 SparkSession 对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    f6(spark)
    //释放资源
    spark.stop()
  }


  def f6(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("input/test.json")
    df.createOrReplaceTempView("user")
    import spark.implicits._
    //封装为 DataSet
    val ds: Dataset[User01] = df.as[User01]
    //创建聚合函数
    var myAgeUdaf1 = new MyAveragUDAF1
    //将聚合函数转换为查询的列
    val col: TypedColumn[User01, Double] = myAgeUdaf1.toColumn
    //查询
    ds.select(col).show()
  }

  def f5(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("input/test.json")
    df.createOrReplaceTempView("user")
    //创建聚合函数
    var myAverage = new MyAveragUDAF
    //在 spark 中注册聚合函数
    spark.udf.register("avgAge",myAverage)
    spark.sql("select avgAge(age) from user").show()
  }

  def f4(spark: SparkSession): Unit = {
    val df = spark.read.json("input/test.json")
    spark.udf.register("addName",(x:String)=> "Name:"+x)
    df.createOrReplaceTempView("people")
    spark.sql("Select addName(username),age from people").show()
  }

  def f3(spark: SparkSession): Unit = {
    //RDD=>DataFrame=>DataSet 转换需要引入隐式转换规则，否则无法转换
    //spark 不是包名，是上下文环境对象名
    import spark.implicits._

    //RDD
    val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu", 20)))
    val df1: DataFrame = rdd1.toDF("id", "name", "age")
//    df1.show()
//    val ds1: Dataset[User] = df1.as[User]
//    ds1.show()

//    val df2: DataFrame = ds1.toDF()
//    //RDD 返回的 RDD 类型为 Row，里面提供的 getXXX 方法可以获取字段值，类似 jdbc 处理结果集， 但是索引从0 开始
//    val rdd2: RDD[Row] = df2.rdd
//    rdd2.foreach((a: Row) =>println(a.getString(1)))
//    rdd2.foreach((a: Row) =>println(a.getInt(2)))

    val value: Dataset[User] = rdd1.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()
    value.show()
//    ds1.rdd
  }

  def f2(spark: SparkSession): Unit = {
    //RDD=>DataFrame=>DataSet 转换需要引入隐式转换规则，否则无法转换
    //spark 不是包名，是上下文环境对象名
    import spark.implicits._
    //读取 json 文件 创建 DataFrame {"username": "lisi","age": 18}
    val df: DataFrame = spark.read.json("input/test.json")
//    df.show()
    //SQL 风格语法
    df.createOrReplaceTempView("user")
//    val frame: DataFrame = spark.sql("select avg(age) from user")
    val frame: DataFrame = spark.sql("select * from user")
    frame.show
    //DSL 风格语法
    //df.select("username","age").show()
  }

  def f1(spark: SparkSession): Unit = {
    //RDD=>DataFrame=>DataSet 转换需要引入隐式转换规则，否则无法转换
    //spark 不是包名，是上下文环境对象名
    import spark.implicits._
    //读取 json 文件 创建 DataFrame {"username": "lisi","age": 18}
    val df: DataFrame = spark.read.json("input/test.json")
    //df.show()
    //SQL 风格语法
    df.createOrReplaceTempView("user")
    //spark.sql("select avg(age) from user").show
    //DSL 风格语法
    //df.select("username","age").show()
    //*****RDD=>DataFrame=>DataSet*****
    //RDD
    val rdd1: RDD[(Int, String, Int)] =
    spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu",
      20)))
    //DataFrame
    val df1: DataFrame = rdd1.toDF("id", "name", "age")
    //df1.show()
    //DateSet
    val ds1: Dataset[User] = df1.as[User]
    //ds1.show()
    //*****DataSet=>DataFrame=>RDD*****
    //DataFrame
    val df2: DataFrame = ds1.toDF()
    //RDD 返回的 RDD 类型为 Row，里面提供的 getXXX 方法可以获取字段值，类似 jdbc 处理结果集， 但是索引从0 开始
    val rdd2: RDD[Row] = df2.rdd
    //rdd2.foreach(a=>println(a.getString(1)))
    //*****RDD=>DataSet*****
    rdd1.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()
    //*****DataSet=>=>RDD*****
    ds1.rdd
  }
}
