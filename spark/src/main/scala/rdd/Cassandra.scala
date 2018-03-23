package rdd

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

/**
  * Created by Chong AiZhen on 18-3-23,下午4:50. 
  */
object Cassandra {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.cassandra.connection.host", "192.168.7.206")
      .set("spark.cassandra.connection.port", "9042")
      .setAppName("cassandra").setMaster("local")
    val sc = new SparkContext(conf)

    //读取上一次的时间戳
    val ts:Long = sc.textFile("hdfs://localhost:9000/result/part-00000").collect()(0).toLong
    println(ts)

    val rdd = sc.cassandraTable("cassandra", "ts_kv_cf").where(" ts > ?",ts)

    rdd.collect().foreach(row => println(s"Existing Data: $row"))

    rdd.saveToCassandra("bigdata", "spark", SomeColumns("id", "ts", "value"))

    sc.stop()
  }
}
