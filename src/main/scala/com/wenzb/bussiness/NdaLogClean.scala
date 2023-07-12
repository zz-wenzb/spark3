package com.wenzb.bussiness

import com.wenzb.dto.Message
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.{Json, parseJson}

/**
 * nda日志清洗.
 */
object NdaLogClean {

  def main(args: Array[String]): Unit = {
    message()
  }

  /**
   * 获取收件人-消息内容
   */
  def message(): Unit = {
    val context: SparkContext = init()
    val rdd: RDD[String] = context.textFile("datas/nda_info-2023-03-21.0.log")
    rdd.filter(_.contains("发消息入参-->"))
      .map((line: String) => {
        val str: String = line.substring(line.indexOf("发消息入参-->") + 8)
        val message1: Message = json2Obj(str)
        (message1.msg.en.message,message1.touser)
      })
      .flatMap((m: (String, List[String])) =>{
          m._2.map((_,m._1))
      })
      .collect().foreach(println)

    destory(context)
  }

  def json2Obj(str: String): Message = {
    val JValue: JValue = parseJson(str)
    implicit val formats = DefaultFormats
    JValue.extract[Message]
  }

  /**
   * (https://api-shumei.hillinsight.tech/api/userinfo/get,805ms)
   * (https://api-shumei.hillinsight.tech/api/org/getList,896ms)
   * (https://api-shumei.hillinsight.tech/api/org/getOrgUser,941ms)
   * (https://api-shumei.hillinsight.tech/api/userinfo/all,720ms)
   * (https://api-shumei.hillinsight.tech/api/userinfo/getList,906ms)
   */
  def maxTimeOfUrl(): Unit = {
    val context: SparkContext = init()
    val rdd: RDD[String] = context.textFile("datas/nda_info-2023-03-21.0.log")
    rdd.filter(_.contains("https://api-shumei.hillinsight.tech"))
      .map((line: String) => {
        val strArr: Array[String] = line.split(" ")
        val value: List[String] = strArr.toList.takeRight(2)
        val str: String = value.head
        var end: Int = str.length
        if (str.indexOf("?") != -1) {
          end = str.indexOf("?")
        }
        val key: String = str.substring(0, end)
        (key, value(1))
      })
      .filter((v: (String, String)) => v._1 != "=>")
      .groupByKey().map((v: (String, Iterable[String])) => {
      val key: String = v._1
      val value: Iterable[String] = v._2
      (key, value.toList.max)
    })
      .collect().foreach(println)

    destory(context)
  }

  def init(): SparkContext = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("nda")
    new SparkContext(sparkConf)
  }

  def destory(context: SparkContext): Unit = {
    context.stop()
  }
}
