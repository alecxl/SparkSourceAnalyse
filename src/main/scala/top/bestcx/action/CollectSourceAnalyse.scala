package top.bestcx.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object CollectSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-collect")
    val sc = new SparkContext(conf)

    val sourceRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRDD: RDD[Int] = sourceRDD.map(x => x * 10)
    println(mapRDD.collect().mkString(","))

    sc.stop()
  }
}
