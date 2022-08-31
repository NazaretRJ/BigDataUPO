import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col}

case class SensorValue(sensor: String, date: String, values: Array[String] )
  
object Ejercicio1 {
  
  
  def main(args: Array[String]):Unit = {
    
    val conf = new SparkConf().setAppName("Ejercicio1P2").setMaster("local")
    val sc = new SparkContext(conf)
    val filerdd = sc.textFile("./consumo.csv").cache() //Data es un RDD que tiene el fichero de texto distribuido
    val notHeaderRDD = filerdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val aux = notHeaderRDD.map(row => row.split(",").map(field => field.trim))
    
    val database = aux.map(row=>
      {
          SensorValue(row(0), row(1), row.slice(2, row.length))
      }
      )
      
      println("AA")
      val sensorRdd = database.filter(_.sensor == "DG1000420")
      sensorRdd.foreach(println)

//      aux.orderBy(col("date").asc).show(false)
      println("FIN")
      
      
//    val processed = rddFromFile.map(f=>{
//      f.split(",")
//    })
        //val processed = rddFromFile.map(row => row.split(",").map(field => field.trim))
    
//    val rdd = processed.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
//
//                    
//    val data = rdd.filter(f => f(0).toString() == "DG1000420").collect
//    
//    data.foreach(println)

    //val wc = data.flatMap(_.split(",")).map((_,1)).reduceByKey(_ + _) //código del contador de palabras
    // map tiene la palabra y un 1. Al hacer reduce junta las palabras y suma los valores
    
    //wc.foreach(println) //imprimo todos los elementos del RDD wc
    
   
  }
}