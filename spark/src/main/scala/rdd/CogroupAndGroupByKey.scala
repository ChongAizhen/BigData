package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/26.
  */
object CogroupAndGroupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CogroupAndGroupByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    val rdd3 = rdd1.join(rdd2)
    rdd3.collect.foreach(println)
    //注意cogroup与groupByKey的区别
    val rdd4 = rdd1.cogroup(rdd2)
    rdd4.collect.foreach(println)
    val rdd5 = (rdd1 union rdd2).groupByKey()
    rdd5.collect.foreach(println)
  }
}
