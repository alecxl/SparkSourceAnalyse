package top.bestcx.cache

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: 曹旭
 * @date: 2020/9/15 3:25 下午
 * @description:
 */
object CacheSourceAnalyse {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-cache")
    val sc = new SparkContext(conf)
    val sourceRDD: RDD[String] = sc.textFile("in/word*.txt")
    val flatMapRDD = sourceRDD.flatMap(x => x.split(" "))
    val mapRDD = flatMapRDD.map(x => (x, 1))
    val resRDD = mapRDD.reduceByKey(_ + _)
    val cache: resRDD.type = resRDD.cache()
    // 测试缓存的效率
    resRDD.saveAsTextFile("output-cache")
    resRDD.saveAsTextFile("output-cache2")
    // 将缓存清除
    resRDD.unpersist(true)
    // 查看将缓存清除后执行效率
    resRDD.saveAsTextFile("output-cache3")
    // 让程序睡一会儿   便于查看详细信息
    Thread.sleep(Long.MaxValue)
    sc.stop()
  }
}
