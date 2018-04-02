package project.project1.src

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/28.
  */
object MobileLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MobileLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("E:\\IdeaProjects\\git\\BigData\\spark\\src\\main\\scala\\project\\project1\\data\\bs_log")
    println(rdd1.partitions.length)

    val rdd2 = rdd1.map(a => {
      val arr = a.split(",")
      val userAndTime = if(arr(3).equals("1")){
        (arr(0)+"_"+arr(2),-arr(1).toLong)
      }else{
        (arr(0)+"_"+arr(2),arr(1).toLong)
      }
      userAndTime
    })

    val rdd3 = rdd2.reduceByKey(_+_)

    rdd3.collect().foreach(println)

    val rdd4 = rdd3.sortBy(_._2,false).groupBy(_._1.split("_")(0))

    rdd4.collect().foreach(println)


    val lac = sc.textFile("E:\\IdeaProjects\\git\\BigData\\spark\\src\\main\\scala\\project\\project1\\data\\lac_info.txt")
    .map(_.split(",")).map(x => (x(0),x(1),x(2)))

    sc.stop()
  }
}
