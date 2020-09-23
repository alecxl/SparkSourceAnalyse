package top.bestcx

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 9:55 上午
 * @description:
 */
object TextFileSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val fileRDD: RDD[String] =
      sparkContext.textFile(
        "in",5)
    fileRDD.saveAsTextFile("out")
    sparkContext.stop()
  }

}
