package mllib

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * Created by Chong AiZhen on 18-10-16,下午6:25. 
  */

//踢足球|天气|温度|湿度|风速
//是(1)否(0)| 晴天(0)阴天(1)下雨(2)|热(0)舒适(1)冷(2)|不适(0)适合(1)|低(0)高(1)
object NaiveBayesFootball {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NaiveBayesFootball").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/home/user/IdeaProjects/github/BigData/data/mllib/NaiveBayesFootball.txt")
    val parsedData =data.map {
      line =>
        val parts =line.split(',')
        LabeledPoint(parts(0).toDouble,Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }
    //样本划分train和test数据样本60%用于train
    val splits = parsedData.randomSplit(Array(0.6,0.4),seed = 11L)
    val training = splits(0)
    val test = splits(1)
    //获得训练模型,第一个参数为数据，第二个参数为平滑参数，默认为1，可改变
    val model = NaiveBayes.train(training,lambda = 1.0)
    //对测试样本进行测试
    //对模型进行准确度分析
    val predictionAndLabel= test.map(p => (model.predict(p.features),p.label))
    val accuracy =1.0 *predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    //打印一个预测值
    println("NaiveBayes精度----->" + accuracy)
    //我们这里特地打印一个预测值：假如一天是   晴天(0)凉(2)高(0)高(1) 踢球与否
    println("假如一天是 晴天(0)凉(2)高(0)高(1) 踢球与否:" + model.predict(Vectors.dense(0.0,2.0,0.0,1.0)))

    //保存model
    val ModelPath = "target/NaiveBayesFootball/NaiveBayes_model.obj"
    model.save(sc,ModelPath)
    //val testmodel = NaiveBayesModel.load(sc,ModelPath)
  }

}
