import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Column

case class SensorValue(sensor: String, date: String, values: Array[Double] )
case class SensorAverage(sensor: String, date: String, values: Array[Double], average : Double )
//case class SensorAverageBad(sensor: String, date: String, values: String, average : Double )

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
      
      
      outRdd.foreach(println)
      val outdf = session.createDataFrame(outRdd)

      outdf.write.mode(SaveMode.Overwrite).csv("./out.csv");
    
   
  }
}