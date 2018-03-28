package project.project1.src

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/28.
  */
object MobileLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MobileLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("E:\\IdeaProjects\\git\\BigData\\spark\\src\\main\\scala\\project\\project1\\data")
    println(rdd1.partitions.length)

    sc.stop()
  }
}
