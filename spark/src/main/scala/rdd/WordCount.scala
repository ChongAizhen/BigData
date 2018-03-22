package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/22.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local");
    val sc = new SparkContext(conf);
    val rdd1 = sc.textFile("E:\\IdeaProjects\\git\\BigData\\data\\input\\test1").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .saveAsTextFile("E:\\IdeaProjects\\git\\BigData\\data\\output")
  }
}