package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/26.
  */
object UnionAndIntersection {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UnionAndIntersection").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(5, 6, 4, 3))
    val rdd2 = sc.parallelize(List(1, 2, 3, 4))
    //求并集
    val rdd3 = rdd1.union(rdd2)
    //求交集
    val rdd4 = rdd1.intersection(rdd2)
    //去重
    rdd3.distinct.collect.foreach(println)
    rdd4.collect.foreach(println)

    sc.stop()
  }
}
