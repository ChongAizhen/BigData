package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/25.
  */
object CatchAndCheckpoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CatchAndCheckpoint").setMaster("local");
    val sc = new SparkContext(conf);
    val rdd = sc.textFile("/home/user/IdeaProjects/github/BigData/data/input/test1").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortByKey()
    rdd.cache()
    sc.setCheckpointDir("/home/user/IdeaProjects/github/BigData/data/checkpoint")
    rdd.checkpoint()
    rdd.collect().foreach(println)
    rdd.saveAsTextFile("/home/user/IdeaProjects/github/BigData/data/output")
    sc.stop()
  }
}
