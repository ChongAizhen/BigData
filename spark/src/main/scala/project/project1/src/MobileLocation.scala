package project.project1.src

import java.text.SimpleDateFormat

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

    val rdd4 = rdd3.map(x=>(x._1.split("_")(0),x._1.split("_")(1),x._2))

    rdd4.collect().foreach(println)

    val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
    val rdd9 = sc.textFile("E:\\IdeaProjects\\git\\BigData\\spark\\src\\main\\scala\\project\\project1\\data\\bs_log").map(_.split(",")).map(x => {
      //（手机号_基站ID，时间，事件类型）
      (x(0) + "_" + x(2), sdf.parse(x(1)).getTime, x(3))
      //按 手机号_基站ID 分组
    }).groupBy(_._1).mapValues(_.map(
        //建立连接基站的时间设置为负的
        x => if (x._3.toInt == 0) x._2.toLong else -x._2.toLong
    )).mapValues(_.sum)

    //rdd9与rdd3的格式相同
    rdd9.collect().foreach(println)

    val rdd10 = rdd9.groupBy(_._1.split("_")(0)).map { case (k, v) => {
      //分组后二次排序
      (k, v.toList.sortBy(_._2).reverse(0))
    }
    }

    rdd10.collect().foreach(println)

    val rdd11 = rdd10.map(t => (t._1, t._2._1.split("_")(1), t._2._2))

    rdd11.collect().foreach(println)

    val lac = sc.textFile("E:\\IdeaProjects\\git\\BigData\\spark\\src\\main\\scala\\project\\project1\\data\\lac_info.txt")
    .map(_.split(",")).map(x => (x(0),x(1),x(2)))

    sc.stop()
  }
}
