import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Main extends App {
  val conf = new SparkConf().setAppName("epsi").setMaster("local")
  val sc = new SparkContext(conf)

  val fileRdd1 = sc.textFile("./books_large_p1.txt")
  val fileRdd2 = sc.textFile("./books_large_p2.txt")
  val fileRdd = fileRdd1.union(fileRdd2)
  val splittedRdd = fileRdd.flatMap(l => l.split("isbn :"))

  println(splittedRdd.count())
  println(splittedRdd.getClass)
  //splittedRdd.foreach(f=> println(f))
  println("HAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n\n\n\n\n\n\n\n\n\n\n")
  println(splittedRdd)
  sc.stop()
}