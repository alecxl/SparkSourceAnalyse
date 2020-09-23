package top.bestcx.exer

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/18 4:30 下午
 * @description:
 */
object UserExer {

  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("exer1")
    val sc = new SparkContext(sparkConf)
    val sourceRDD: RDD[String] = sc.textFile("in/teacher.log")
    val mapRDD: RDD[((String, String), Int)] = sourceRDD.map(f = lines => {
      //  http://java.cc.com/zs
      val fields: Array[String] = lines.split("/")
      val length = fields.length
      val course: Array[String] = fields(length - 2).split("\\.")
      val teacher = fields(length - 1)
      ((course(0),teacher),1)
    })
    val groupRDD: RDD[((String, String), Iterable[Int])] = mapRDD.groupByKey()
    val mapValueRDD = groupRDD.mapValues(iter => {
      iter.sum
    }).map(t => {
      (t._1._1, t._1._2, t._2)
    })
    println(mapValueRDD.collect().toBuffer)
    sc.stop()
  }

}
