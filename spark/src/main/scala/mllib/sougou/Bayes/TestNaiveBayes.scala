package mllib.sougou.Bayes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row

/**
  * Created by Chong AiZhen on 18-11-1,下午3:05. 
  */
object TestNaiveBayes {

  case class RawDataRecord(category: String, text: String)

  def main(args : Array[String]) {

//    val ss = SparkSession.builder().appName("Spark In Action").master("local").getOrCreate()

    val conf = new SparkConf().setAppName("TestNaiveBayes").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    var srcRDD = sc.textFile("/home/user/IdeaProjects/github/BigData/data/mllib/sougou/test/").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }

    var testDF = srcRDD.toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var testwordsData = tokenizer.transform(testDF)

    //计算每个词在文档中的词频
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")

    var testfeaturizedData = hashingTF.transform(testwordsData)

    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(testfeaturizedData)
    var testrescaledData = idfModel.transform(testfeaturizedData)

    //测试数据集，做同样的特征表示及格式转换
    var testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //加载模型
    val model = NaiveBayesModel.load(sc, "/home/user/IdeaProjects/github/BigData/data/mllib/sougou/model")

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features)))

    println("-----------------------------------oupput:")
    testpredictionAndLabel.collect().foreach(println)

  }
}
