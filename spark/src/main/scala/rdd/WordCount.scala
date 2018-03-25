package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/22.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setMaster("local");
    val sc = new SparkContext(conf);
    val rdd1 = sc.textFile("E:\\IdeaProjects\\git\\BigData\\data\\input\\test1")
      /*
       data 有两行数据，第一行 a,b,c，第二行1,2,3
       scala>data.map(line1 => line1.split(",")).collect()
       res11: Array[Array[String]] = Array(Array(a, b, c),Array(1, 2, 3))
       scala>data.flatMap(line1 => line1.split(",")).collect()
       res12: Array[String] = Array(a, b, c, 1, 2, 3)
       */
      .flatMap(_.split(" "))
      .map((_, 1))
      //reduceByKey(x,y=>x+y)
      .reduceByKey(_ + _)
      //按照第二个值的大小降序排序
      .sortBy(_._2,false)
      .saveAsTextFile("E:\\IdeaProjects\\git\\BigData\\data\\output")
    sc.stop()
  }
}