package project.MobileLocation.src

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/28.
  */
object MobileLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MobileLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

//    val rdd1 = sc.textFile("E:\\IdeaProjects\\git\\BigData\\spark\\src\\main\\scala\\project\\project1\\data\\bs_log")
//    println(rdd1.partitions.length)
//
//    val rdd2 = rdd1.map(a => {
//      val arr = a.split(",")
//      val userAndTime = if(arr(3).equals("1")){
//        (arr(0)+"_"+arr(2),-arr(1).toLong)
//      }else{
//        (arr(0)+"_"+arr(2),arr(1).toLong)
//      }
//      userAndTime
//    })
//
//    val rdd3 = rdd2.reduceByKey(_+_)
//
//    rdd3.collect().foreach(println)
//
//    val rdd4 = rdd3.map(x=>(x._1.split("_")(0),x._1.split("_")(1),x._2))
//
//    rdd4.collect().foreach(println)


    //示例代码
    /*
    基站连接手机号，连接时间戳，基站站点ID信息，“1”表示连接，“0”表示断开连接。
    18688888888,20160327082400,16030401EAFB68F1E3CDF819735E1C66,1
     */
    //从log文件拿到数据，并按行采集。
    //sc.textFile("c://information//bs_log").map(_.split(",")).map(x => (x(0), x(1), x(2), x(3)))
    val rdd_Info = sc.textFile("/home/user/IdeaProjects/github/BigData/spark/src/main/scala/project/project1/data/bs_log").map(line => {
      //通过“，”将数据进行切分field(0)手机号，field(1)时间戳，field(2)基站ID信息，field(3)事件类型
      val fields = line.split(",")
      //事件类型，“1”表示连接，“0”表示断开。
      val eventType = fields(3)
      val time = fields(1)
      //连接基站将时间戳至为“-”，断开基站将时间戳至为“+”，以便后面进行计算。
      val timeLong = if(eventType == "1") -time.toLong else time.toLong
      //构成一个数据类型(手机号，基站ID信息，带符号的时间戳)
      ((fields(0),fields(2)),timeLong)
    })
    val rdd_lacInfo = rdd_Info.reduceByKey(_+_).map(t=>{
      val mobile = t._1._1
      val lac = t._1._2
      val time = t._2
      (lac, (mobile, time))
    })
    val rdd_coordinate = sc.textFile("/home/user/IdeaProjects/github/BigData/spark/src/main/scala/project/project1/data/lac_info.txt").map(line =>{
      val f = line.split(",")
      //（基站ID， （经度， 纬度））
      (f(0),(f(1), f(2)))
    })
    //rdd1.join(rdd2)-->(CC0710CC94ECC657A8561DE549D940E0,((18688888888,1300),(116.303955,40.041935)))
    val rdd_all = rdd_lacInfo.join(rdd_coordinate).map(t =>{
      val lac = t._1
      val mobile = t._2._1._1
      val time = t._2._1._2
      val x = t._2._2._1
      val y = t._2._2._2
      (mobile, lac, time, x, y)
    })

    //按照手机号进行分组
    val rdd_mobile = rdd_all.groupBy(_._1)

    //取出停留时间最长的前两个基站（为什么要转成list？？？）
    val rdd_topTwo= rdd_mobile.mapValues(it =>{
      it.toList.sortBy(_._3).reverse.take(2)
    })

        println(rdd_Info.collect().toBuffer)
        println(rdd_lacInfo.collect().toBuffer)
        println(rdd_coordinate.collect().toBuffer)
        println(rdd_all.collect().toBuffer)
        println(rdd_mobile.collect().toBuffer)
        println(rdd_mobile.collect().toList)
        println(rdd_mobile.collect())
        println(rdd_topTwo.collect().toBuffer)
//    rdd_topTwo.saveAsTextFile("j://information//out")



    sc.stop()

  }
}
