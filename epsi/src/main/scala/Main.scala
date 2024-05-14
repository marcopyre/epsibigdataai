import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Pipeline
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.Finisher

object Main extends App {
  val sc: SparkSession =
    SparkSession
      .builder()
      .appName("epsi")
      .config("spark.master", "local")
      .getOrCreate()

  import sc.implicits._

  val fileRdd1 = sc.sparkContext.textFile("./books_large_p1.txt")
  val fileRdd2 = sc.sparkContext.textFile("./books_large_p2.txt")
  val fileRdd = fileRdd1.union(fileRdd2).zipWithIndex

  var skip = false
  var prevIndex = 0
  val bookIndexs = fileRdd.filter(line => line._1 contains "isbn : ")

  val explainPipelineModel = PretrainedPipeline("explain_document_ml").model
  val finisherExplainEn = new Finisher().setInputCols("token", "lemmas", "pos")
  val pipelineExplainEn = new Pipeline().setStages(Array(explainPipelineModel, finisherExplainEn))

  val sentimentPipelineModel = PretrainedPipeline("analyze_sentiment").model
  val finisherSentiment = new Finisher().setInputCols("document","sentiment")
  val pipelineSentiment = new Pipeline().setStages(Array(sentimentPipelineModel,finisherSentiment))

  bookIndexs.foreach(each => {
    val output = fileRdd.filter(line => line._2 > prevIndex && line._2 <= each._2.toInt).map(line => line._1)
    val ds = output.toDS()

    val testSentimentData = Seq("testing this piece of shit").toDF("text")
    val modelSentiment = pipelineSentiment.fit(testSentimentData)
    val sentimentTestSentimentData = modelSentiment.transform(testSentimentData)
    sentimentTestSentimentData.show(false)

    prevIndex = each._2.toInt
  })

  // Stop the SparkSession
  sc.stop()
}
