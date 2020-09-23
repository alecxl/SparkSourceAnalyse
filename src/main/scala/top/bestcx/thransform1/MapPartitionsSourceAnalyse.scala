package top.bestcx.thransform1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object MapPartitionsSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-mapPartition")
    val sc = new SparkContext(conf)
    val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRdd: RDD[Int] = sourceRdd.mapPartitions(
      iter => iter.filter( _ % 2 ==0)
    )
    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
