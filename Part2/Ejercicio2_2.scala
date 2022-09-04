import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types._

object Ejercicio2_2 {
 
  def main(args: Array[String]):Unit = {
    // Load and parse the data file, converting it to a DataFrame. 
    
    var schemaA = new ArrayBuffer[StructField]()
    var sensorValuesA = new ArrayBuffer[String]()
    
    schemaA.append(StructField("date", StringType, false))

    for(i <- 1 to 146)
    {
      schemaA.append(StructField("v"+i.toString, DoubleType, false))
      sensorValuesA.append("v"+i.toString)
    }
        
    
    val spark = SparkSession.builder.appName("Ejercicio2bP2").master("local").getOrCreate()
    
    val customSchema = StructType(schemaA.toArray)
    val data = spark.read.format("csv").schema(customSchema).load("./prediccion.txt")
    
    //creating features column
    val assembler = new VectorAssembler()
      .setInputCols(sensorValuesA.toArray)
      .setOutputCol("sensorValues")
       
    val newData = data.drop("date")
    
    assembler.transform(newData)
    
    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = newData.randomSplit(Array(0.7, 0.3))
  
    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("v146") 
      .setFeaturesCol("sensorValues")
    
    // Train model. This also runs the indexer.
    val model = rf.fit(trainingData)
   
    // Make predictions.
    val predictions = model.transform(testData)
    
    // Select example rows to display.
    predictions.select("prediction", "sensorValues").show(5)
    
    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("v146") 
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")
    
    val rfModel = model.stages(1).asInstanceOf[RandomForestRegressionModel]
    println(s"Learned regression forest model:\n ${rfModel.toDebugString}")
    
  }
}