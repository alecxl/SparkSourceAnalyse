package top.bestcx.thransform1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object MapPartitionsWithIndexSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-mapPartitionWithIndex")
    val sc = new SparkContext(conf)
    val sourceRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRdd: RDD[(Int, Int)] = sourceRdd.mapPartitionsWithIndex(
      (index,iter) =>{
        val tuples = iter.map(i => {
          println(index)
          (index, i)
        })
        tuples
      }
    )
    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
