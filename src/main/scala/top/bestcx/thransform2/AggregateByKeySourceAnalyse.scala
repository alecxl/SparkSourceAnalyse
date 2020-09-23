package top.bestcx.thransform2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object AggregateByKeySourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-aggregateByKey")
    val sc = new SparkContext(conf)

    val sourceRDD: RDD[(String, Int)] = sc.makeRDD(List(
      ("spark", 1), ("hadoop", 2), ("scala", 2), ("hive", 3),
      ("spark", 3), ("hadoop", 1), ("scala", 11), ("hive", 2),
      ("hive", 1)
    ),3)
    val foldRDD: RDD[(String, Int)] = sourceRDD.aggregateByKey(10)(_+_,_+_)
    foldRDD.saveAsTextFile("output-aggregateByKey")
    sc.stop()
  }
}
