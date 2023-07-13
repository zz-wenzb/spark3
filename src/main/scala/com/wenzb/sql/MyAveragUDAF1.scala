package com.wenzb.sql

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator


class MyAveragUDAF1 extends Aggregator[User01, AgeBuffer, Double] {
  override def zero: AgeBuffer = {
    AgeBuffer(0L, 0L)
  }

  override def reduce(b: AgeBuffer, a: User01): AgeBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(buff: AgeBuffer): Double = {
    buff.sum.toDouble / buff.count
  }

  //DataSet 默认额编解码器，用于序列化，固定写法
  //自定义类型就是 product 自带类型根据类型选择
  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }

}
