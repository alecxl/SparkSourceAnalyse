package top.bestcx.thransform2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object DistinctSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-distinct")
    val sc = new SparkContext(conf)

    val sourceRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 2, 4), 2)
    val disRDD: RDD[Int] = sourceRDD.distinct()
    println(disRDD.collect().toBuffer)
    sc.stop()
  }
}
