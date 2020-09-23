package top.bestcx.thransform1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object MapSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-map")
    val sc = new SparkContext(conf)
    val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRdd: RDD[Int] = sourceRdd.map(_ * 10)
    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
