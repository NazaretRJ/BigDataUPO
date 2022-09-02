import org.apache.spark.rdd.RDD
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
    
    val session = SparkSession.builder()
      .master("local")
      .appName("Ejercicio1P2")
      .getOrCreate();
    
    import session.implicits._
    
    val dataframe = session.read.options(Map("header"->"true")).options(Map("delimiter"->",")).format("csv").load("./consumo.csv").cache()
   
    val rdd = dataframe.rdd
    var array = new ArrayBuffer[Double]()
    val classrdd = rdd.map(row=>
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
      
      var rowS = "" 
      
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
      
      val file = new File("./out.csv")
      val bw = new BufferedWriter(new FileWriter(file))
      
      outRdd.take(2).foreach(row => 
        {
          bw.write(row.sensor)
          bw.write(" , ")
          bw.write(row.date);
          for(i <- 0 until row.values.size)
          {
            bw.write(" , ")
            bw.write(row.values(i).toString)
          }
          bw.newLine()
          bw.flush()
        }
        )
    bw.close()
   
  }
}