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
    val spark = SparkSession.builder.appName("Ejercicio2bP2").master("local").getOrCreate()

    val data = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true")).format("csv").load("./prediccion.txt")
    val sensorCl = data.schema.map(sch => sch.name).filter(_!="Date").filter(_!="Average")

    val assembler = new VectorAssembler()
      .setInputCols(sensorCl.toArray)
      .setOutputCol("features")

    val assemblerDf = assembler.transform(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = assemblerDf.randomSplit(Array(0.7, 0.3))


    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("Average")
      .setFeaturesCol("features")


    // Train model. This also runs the indexer.
    val model = rf.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    val predictionsShow = predictions.select("prediction", "Average","Date", "features")
    predictionsShow.show()

    val evaluator = new RegressionEvaluator().setLabelCol("Average").setPredictionCol("prediction").setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)

    println("Root Mean Squared Error (RMSE) on test data: "+rmse)   
    
  }
}