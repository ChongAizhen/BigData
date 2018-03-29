package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/28.
  */
object JoinAndCartesian {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JoinAndCartesian").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("tom", 1), ("jerry", 2), ("kitty", 3)))
    val rdd2 = sc.parallelize(List(("jerry", 9), ("tom", 8), ("shuke", 7)))

    //join
    val rdd3 = rdd1.join(rdd2)
    rdd3.collect().foreach(println)
    val rdd4 = rdd1.leftOuterJoin(rdd2)
    rdd4.collect().foreach(println)
    val rdd5 = rdd1.rightOuterJoin(rdd2)
    rdd5.collect().foreach(println)

    //cartesian笛卡尔积
    val rdd6 = rdd1.cartesian(rdd2)
    rdd6.collect().foreach(println)

    sc.stop()
  }
}
