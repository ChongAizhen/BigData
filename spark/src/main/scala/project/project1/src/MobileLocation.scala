package project.project1.src

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/28.
  */
object MobileLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MobileLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("/home/user/IdeaProjects/github/BigData/spark/src/main/scala/project/project1/data/bs_log")
    println(rdd1.partitions.length)


    val rdd2 = rdd1.map(a => {
      val arr = a.split(",")
      val rdd3 = if(arr(3).equals("1")){
        (arr(0)+"_"+arr(2),arr(1))
      }else{
        (arr(0)+"_"+arr(2),arr(1))
      }
      rdd3
    })

    sc.stop()
  }
}
