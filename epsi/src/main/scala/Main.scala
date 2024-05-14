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

  val sentimentPipelineModel = PretrainedPipeline("analyze_sentiment").model
  val finisherSentiment = new Finisher().setInputCols("document","sentiment")
  val pipelineSentiment = new Pipeline().setStages(Array(sentimentPipelineModel,finisherSentiment))

  import sc.implicits._

  val numPartitions = 200
  val fileRdd1 = sc.sparkContext.textFile("./books_large_p1.txt", numPartitions)
  val fileRdd2 = sc.sparkContext.textFile("./books_large_p2.txt", numPartitions)
  val fileRdd = fileRdd1.union(fileRdd2).zipWithIndex

  var skip = false
  var prevIndex = 0
  val bookIndexs = fileRdd.filter(line => line._1 contains "isbn : ").collect

  bookIndexs.foreach(each => {
    val output = fileRdd.filter(line => line._2 > prevIndex && line._2 <= each._2.toInt).map(line => line._1)
    val text = output.toDS().collect.mkString("")
    val testSentimentData = Seq(text).toDF("text").cache()
    val modelSentiment = pipelineSentiment.fit(testSentimentData)
    val sentimentTestSentimentData = modelSentiment.transform(testSentimentData)
    sentimentTestSentimentData.show

    prevIndex = each._2.toInt
  })

  sc.stop()
}
