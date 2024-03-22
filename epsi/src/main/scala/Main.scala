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
  val pipelineExplainEn = new Pipeline().setStages(Array(explainPipelineModel,finisherExplainEn))

  bookIndexs.foreach(each => {
    val output = fileRdd.filter(line => line._2 > prevIndex && line._2 <= each._2.toInt).map(line => line._1)
    val ds = output.toDS()
    val dsRenamed = ds.withColumnRenamed("value", "text")

    val modelExplainEn = pipelineExplainEn.fit(dsRenamed)
    val annoteTexteEn = modelExplainEn.transform(dsRenamed).cache()
    annoteTexteEn.show()
    prevIndex = each._2.toInt
  })

  sc.stop()
}