package top.bestcx.thransform1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object FlatMapSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-filter")
    val sc = new SparkContext(conf)
    val sourceRdd: RDD[String] = sc.makeRDD(List("hello spark","hello hadoop", "hello spark"))
    val flatMapRDD: RDD[String] = sourceRdd.flatMap(value => {
      value.split(" ")
    })
    println(flatMapRDD.collect().mkString(","))
    sc.stop()
  }
}
