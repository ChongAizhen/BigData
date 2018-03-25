package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/25.
  */
object CatchAndCheckpoint {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CatchAndCheckpoint").setMaster("local");
    val sc = new SparkContext(conf);
    val rdd = sc.textFile("").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortByKey()
    rdd.cache()
    sc.setCheckpointDir("checkpoint存放的目录")
    rdd.checkpoint()
    rdd.collect()
    rdd.saveAsTextFile("")
    sc.stop()
  }
}
