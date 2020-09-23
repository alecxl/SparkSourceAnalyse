package top.bestcx.thransform3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object InterSectionSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-intersection")
    val sc = new SparkContext(conf)
    val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 5, 7),2)
    val sourceRdd2: RDD[Int] = sc.makeRDD(List(2, 5, 6, 8, 10),2)
    val interRdd: RDD[Int] = sourceRdd.intersection(sourceRdd2)

    println(interRdd.collect().toBuffer)
    sc.stop()
  }
}
