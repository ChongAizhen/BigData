package rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chongaizhen on 2018/03/22.
  */

//给定一个文本，计算文本的总长度
object TotalLenth {
  def main(args: Array[String]): Unit = {
    /*
    java版：
    JavaRDD<String> lines = sc.textFile("hdfs://master:9000/testFile/README.md");

    //第一个参数为传入的内容，第二个参数为函数操作完后返回的结果类型
    JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
    public Integer call(String s) {
        System.out.println("每行长度"+s.length());
        return s.length(); }
    });

    int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer a, Integer b) { return a + b; }
    });

    System.out.println(totalLength);
     */
    val conf = new SparkConf().setAppName("TotalLenth").setMaster("local");
    val sc = new SparkContext(conf);
    val rdd1=sc.textFile("E:\\IdeaProjects\\git\\BigData\\data\\input\\test1").map(_.length).reduce(_+_)
    println(rdd1)
    sc.stop()
  }
}
