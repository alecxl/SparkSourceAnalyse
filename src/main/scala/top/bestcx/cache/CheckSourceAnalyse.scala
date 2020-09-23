package top.bestcx.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object CheckSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-check")
    val sc = new SparkContext(conf)

    val sourceRDD: RDD[String] = sc.textFile("in/user_info.txt")
    sc.setCheckpointDir("output-check1")
    val filterRDD: RDD[String] = sourceRDD.filter(_.length > 5)
    filterRDD.checkpoint()
    filterRDD.saveAsTextFile("output-check2")

    filterRDD.saveAsTextFile("output-check3")
    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
