import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Column
import java.io._ 

case class SensorValue(sensor: String, date: String, values: Array[Double] )
case class SensorAverage(sensor: String, date: String, values: Array[Double], average : Double )

object Ejercicio1 {
  
  
  def main(args: Array[String]):Unit = {
    
    val conf = new SparkConf().setAppName("Ejercicio1P2").setMaster("local")
    val sc = new SparkContext(conf)
    val fileRdd = sc.textFile("./consumo.csv").cache() //RDD que tiene el fichero de texto distribuido
    val notHeaderRDD = fileRdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val notDelimiterRdd = notHeaderRDD.map(row => row.split(",").map(field => field.trim))
 
    
    var array = new ArrayBuffer[Double]()
    
    val classrdd = notDelimiterRdd.map(row=>
      {
        array = ArrayBuffer[Double]()
        for(i <- 2 until row.size)
        {
          if(row(i) == null || row(i) == "")
          {
            array.append(0.0)
          }
          else
          {
            array.append(row(i).toString.toDouble)
          }
        }
          SensorValue(row(0).toString(), row(1).toString(), array.toArray)
      }
      )
      
      val sensorRdd = classrdd.filter(_.sensor == "DG1000420").sortBy(_.date, false)
      
      var average = 0.0
      var currentAverage = 0.0
      
      /*
       * We loop twice the same RDD to calculate the average and then to write in the file.
       * 
       * Pros: This way: we can order the outRdd by another field.
       * Cons: We are doing more processing and we need more memory
       
      val outRdd = sensorRdd.map( row =>
      {
        if(currentAverage == 0 || row.values.size == 0)
        {
          average = 0.0
        }
        else
        {
          average = currentAverage / row.values.size.toDouble
        }
         
        currentAverage = 0.0
         
        for(i <- 0 until row.values.size)
        {
          currentAverage = currentAverage + row.values(i)
        }
         SensorAverage(row.sensor, row.date, row.values, average)
      }
      
      )
      
      var header = new ArrayBuffer[String]()
      val max = classrdd.first().values.length
      header.append("Date")
      header.append(",")
      for(i <- 1 to max)
      {
        header.append("V"+i.toString);
        header.append(",")
      }
      
      header.append("Average")
      
     val file = new File("./prediccion.txt")
     val bw = new BufferedWriter(new FileWriter(file))
     
      header.toList.foreach(x =>
        {
          bw.write(x)
        })
        bw.newLine()
        bw.flush()
        
      sensorRdd.collect.foreach( row =>
      {
        if(currentAverage == 0 || row.values.size == 0)
        {
          average = 0.0
        }
        else
        {
          average = currentAverage / row.values.size.toDouble
        }
         
        currentAverage = 0.0
        
        bw.write(row.date);
        bw.write(",")
          
        for(i <- 0 until row.values.size)
        {
          currentAverage = currentAverage + row.values(i)
          
          bw.write(row.values(i).toString)
          bw.write(",")  
        }
        
        bw.write(average.toString)
        
        bw.newLine()
        bw.flush()
      }
      
      )
    bw.close()
    * */
    /*
     * We calculate the average while we are saving in the file.
     * Pros: We don't create another RDD and we don't loop twice. So we are saving resources.
     * Cons: It is limited to the order.
     * */
     
      var header = new ArrayBuffer[String]()
      val max = classrdd.first().values.length
      header.append("Date")
      header.append(",")
      for(i <- 1 to max)
      {
        header.append("V"+i.toString);
        header.append(",")
      }
      
      header.append("Average")
      
     val file = new File("./prediccion.txt")
     val bw = new BufferedWriter(new FileWriter(file))
     
      header.toList.foreach(x =>
        {
          bw.write(x)
        })
        bw.newLine()
        bw.flush()
        
      sensorRdd.collect.foreach( row =>
      {
        if(currentAverage == 0 || row.values.size == 0)
        {
          average = 0.0
        }
        else
        {
          average = currentAverage / row.values.size.toDouble
        }
         
        currentAverage = 0.0
        
        bw.write(row.date);
        bw.write(",")
          
        for(i <- 0 until row.values.size)
        {
          currentAverage = currentAverage + row.values(i)
          
          bw.write(row.values(i).toString)
          bw.write(",")  
        }
        
        bw.write(average.toString)
        
        bw.newLine()
        bw.flush()
      }
      
      )
      
      bw.close()
    
  }
}
