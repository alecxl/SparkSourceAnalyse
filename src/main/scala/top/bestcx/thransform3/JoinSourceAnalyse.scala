package top.bestcx.thransform3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object JoinSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-join")
    val sc = new SparkContext(conf)

    val sourceRdd: RDD[(String, Int)] = sc.makeRDD(List(("scala",2),("hadoop",3),("hive",1)),2)
    val sourceRdd2: RDD[(String, Int)] = sc.makeRDD(List(("scala",4),("hadoop",2),("spark",5),("hadoop",4)),2)

    val joinRdd: RDD[(String, (Int, Int))] = sourceRdd.join(sourceRdd2)
    val leftOuterJoinRdd: RDD[(String, (Int, Option[Int]))] = sourceRdd.leftOuterJoin(sourceRdd2)
    val rightOuterJoinRdd: RDD[(String, (Option[Int], Int))] = sourceRdd.rightOuterJoin(sourceRdd2)
    val fullOuterJoinRdd: RDD[(String, (Option[Int], Option[Int]))] = sourceRdd.fullOuterJoin(sourceRdd2)
    val value = sourceRdd.cogroup(sourceRdd2)
    println(value.collect().toBuffer)
//    println("joinRdd result:")
//    println(joinRdd.collect().toBuffer)
//    println("leftOuterJoin result:")
//    println(leftOuterJoinRdd.collect().toBuffer)
//    println("rightOuterJoin result:")
//    println(rightOuterJoinRdd.collect().toBuffer)
//    println("fullOuterJoin result:")
//    println(fullOuterJoinRdd.collect().toBuffer)

    sc.stop()
  }
}
