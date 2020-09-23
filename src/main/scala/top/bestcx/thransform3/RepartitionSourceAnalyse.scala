package top.bestcx.thransform3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object RepartitionSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-repartition")
    val sc = new SparkContext(conf)


    val sourceRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val repartitionRDD: RDD[Int] = sourceRDD.repartition(3)
    //sourceRDD.coalesce(2)
    repartitionRDD.saveAsTextFile("output-repartition")


    sc.stop()
  }
}
