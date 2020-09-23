package top.bestcx.thransform2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object ReduceByKeySourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-reduceByKey")
    val sc = new SparkContext(conf)
    val sourceRDD: RDD[String] = sc.makeRDD(List(
      "spark", "hadoop", "hive", "spark",
      "spark", "spark", "flink", "hbase",
      "kafka", "kafka", "kafka", "kafka",
      "hadoop", "flink", "hive", "flink"
    ), 4)

    val mapRDD: RDD[(String, Int)] = sourceRDD.map((_, 1))
    val reduceByKeyRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceByKeyRDD.saveAsTextFile("output-reduceByKey")
    Thread.sleep(
      Long.MaxValue
    )
    sc.stop()
  }
}
