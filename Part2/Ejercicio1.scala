import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.ListBuffer
import java.util.List

case class SensorValue(sensor: String, date: String, values: Array[String] )
case class SensorAverage(sensor: String, date: String, values: Array[String] )

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
      val sensorRdd = database.filter(_.sensor == "DG1000420").sortBy(_.date, false)
      sensorRdd.foreach(println)

//      aux.orderBy(col("date").asc).show(false)
      println("FIN")
      
      //sensorRdd.saveAsTextFile("out.csv")
//      val out = new BufferedWriter(new FileWriter("./out.csv"))
//      val writer = new CSVWriter(out)
//      
//      var average = 0
//      var currentAverage = 0
//      val list = sensorRdd.collect()
//      var sensorList = Array[String]()
//      var bigList = ListBuffer[Array[String]]()
//        
//      for( sensor <- sensorRdd)
//      {
//         sensorList :+ sensor.sensor
//         sensorList :+ sensor.date
//         
//         for(value <- sensor.values)
//         {
//           sensorList :+ value
//           currentAverage = value.toInt + currentAverage
//         }
//         currentAverage = currentAverage / sensor.values.size
//         sensorList :+ average.toString
//         average = currentAverage       
//         
//         bigList += sensorList
//         
//         sensorList = Array[String]()
//      }
//      val auxList = bigList.toList
//      
//      writer.writeAll()
      
      
      //      var average = 0
//      var previous = sensorRdd.first
//      var first = true
//      val averageRdd = sensorRdd.map( row =>
//      {
//        if(first)
//        {
//          first = false
//          SensorAverage(row(0), row(1), row.slice(2, row.length))
//        }
//        else{
//          foreach( value, row.slice(2, row.length))
//          {
//            
//          }
//        }
//        
//        previous = row
//      })
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