import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.StringBuilder

object Main extends App {
  val conf = new SparkConf().setAppName("epsi").setMaster("local")
  val sc = new SparkContext(conf)

  val fileRdd1 = sc.textFile("./books_large_p1.txt")
  val fileRdd2 = sc.textFile("./books_large_p2.txt")
  val fileRdd = fileRdd1.union(fileRdd2)
  var skip = false
  var memory = ""
  var Books:Map[String,String] = Map()

  val sb = new StringBuilder()

  fileRdd.foreach(line => {
    if(line contains "isbn : "){
      if(skip == false){
        var isbn = ""
        var running = true
        for(lettre <- line.substring(line.indexOf("isbn : ") + 7); if running){
          if(!lettre.isSpaceChar){
            isbn += lettre.toString
          }
          else{
            running = false
          }
        }
        println(isbn)
        //println(sb.toString())
        sb.setLength(0)
      }
      else{
        skip = true
      }
    }
    sb.append(line.toString);
    //memory = memory.concat(line.toString)
  })
  sc.stop()
}