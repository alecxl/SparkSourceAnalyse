package top.bestcx.thransform3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object CogroupSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-cogroup")
    val sc = new SparkContext(conf)

    val source1: RDD[(String, Int)] = sc.makeRDD(List(("spark", 2), ("hadoop", 3), ("scala", 2),("scala", 3)))
    val source2: RDD[(String, Int)] = sc.makeRDD(List(("spark", 1), ("hive", 3), ("scala", 4)))

    val coRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int], Iterable[Int]))] = source1.cogroup(source2,source1,source2)

    println(coRDD.collect().toBuffer)


    sc.stop()
  }
}
