package top.bestcx.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object FoldSourceAnalyse {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setMaster("local[*]").setAppName("spark-fold")
      val sc = new SparkContext(conf)

      val sourceRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
      val res: Int = sourceRDD.fold(100)(_+_)
      println(res)
      sc.stop()
    }
}
