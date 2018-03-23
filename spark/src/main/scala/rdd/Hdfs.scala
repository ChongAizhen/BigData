package rdd

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Chong AiZhen on 18-3-23,下午4:54. 
  */
object Hdfs {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setAppName("jsonParser").setMaster("local")
    val sc = new SparkContext(conf)

    //获取文件中第一行的内容
    val ts:Long = sc.textFile("hdfs://localhost:9000/result/part-00000").collect()(0).toLong
    println(ts)

    val path = new Path("hdfs://localhost:9000/result");
    //得到hdfs对象
    val hdfs = org.apache.hadoop.fs.FileSystem.get(
      new java.net.URI("hdfs://localhost:9000"), new org.apache.hadoop.conf.Configuration())
    //true表示递归删除
    if (hdfs.exists(path)) hdfs.delete(path, true)

    //保存
    val max=sc.parallelize(Seq(ts)).saveAsTextFile("hdfs://localhost:9000/result")

    sc.stop()
  }
}
