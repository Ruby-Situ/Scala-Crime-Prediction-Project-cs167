package edu.ucr.cs.cs167.group11

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.ml.classification.LinearSVCModel
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.sql.functions.col

object CrimeArrestPrediction {
  def main(args : Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage <input file>")
      println("  - <input file> path to a CSV file input")
      sys.exit(0)
    }
    val inputfile = args(0)
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 A5")
      .config(conf)
      .getOrCreate()

    val t1 = System.nanoTime
    try {
      // Load the Parquet file
      val df: DataFrame = spark.read.parquet(inputfile)
      val crimeDF = df.filter(col("Arrest").isin("true", "false"))
      // Tokenize PrimaryType & description column array of words
      val tokenizePrimary = new Tokenizer()
        .setInputCol("PrimaryType")
        .setOutputCol("primaryTokens")
      val tokenizeDesc = new Tokenizer()
        .setInputCol("Description")
        .setOutputCol("descTokens")

      // Turn primaryTokens & descTokens into numeric features
      val hashPrimary = new HashingTF()
        .setInputCol("primaryTokens")
        .setOutputCol("primaryFeatures")
      val hashDesc = new HashingTF()
        .setInputCol("descTokens")
        .setOutputCol("descFeatures")

      // Combine the two features into a single feature vector for later use
      val assembler = new org.apache.spark.ml.feature.VectorAssembler()
        .setInputCols(Array("primaryFeatures", "descFeatures"))
        .setOutputCol("features")

      // Convert Arrest column to numeric labels - 0:false, 1:true
      val indexer = new StringIndexer()
        .setInputCol("Arrest")
        .setOutputCol("label")
        .setHandleInvalid("skip")

      // create an object for the Linear Support Vector Machine classifier
      val svc = new LinearSVC()

      // create a pipeline that includes all the previous transformations and the model
      val pipeline = new Pipeline()
        .setStages(Array(tokenizePrimary, tokenizeDesc, hashPrimary, hashDesc, assembler, indexer, svc))

      // create a parameter grid to cross validate the model on different hyperparameters
      val paramGrid: Array[ParamMap] = new ParamGridBuilder()
        .addGrid(hashPrimary.numFeatures, Array(1024, 2048))
        .addGrid(svc.regParam, Array(0.01, 0.0001))
        .addGrid(svc.maxIter, Array(10, 15))
        .addGrid(svc.threshold, Array(0.0, 0.25))
        .build()

      // create a cross validation job that will process the pipeline using all possible combinations in the parameter grid
      val cv = new TrainValidationSplit()
        .setEstimator(pipeline)
        .setEvaluator(new BinaryClassificationEvaluator())
        .setEstimatorParamMaps(paramGrid)
        .setTrainRatio(0.8)
        .setParallelism(2)

      // Split the data into 80% training and 20% testing
      val Array(trainingData: Dataset[Row], testData: Dataset[Row]) = crimeDF.randomSplit(Array(0.8, 0.2))

      // Run cross-validation, and choose the best set of parameters.
      val model: TrainValidationSplitModel = cv.fit(trainingData)

      // Extract the best hyperparameters from the trained model
      val numFeatures = model.bestModel.asInstanceOf[PipelineModel].stages(2).asInstanceOf[HashingTF].getNumFeatures
      val regParam = model.bestModel.asInstanceOf[PipelineModel].stages(6).asInstanceOf[LinearSVCModel].getRegParam
      val maxIter = model.bestModel.asInstanceOf[PipelineModel].stages(6).asInstanceOf[LinearSVCModel].getMaxIter
      val threshold = model.bestModel.asInstanceOf[PipelineModel].stages(6).asInstanceOf[LinearSVCModel].getThreshold

      println(s"Best model parameters:")
      println(s"numFeatures: $numFeatures")
      println(s"regParam: $regParam")
      println(s"maxIter: $maxIter")
      println(s"threshold: $threshold")

      // apply the model to your test set and show sample of the result
      val predictions: DataFrame = model.transform(testData)
      predictions
        .select("PrimaryType", "Description", "Arrest", "label", "prediction")
        .show(5)

      // evaluate the test results
      val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("prediction")

      val accuracy = binaryClassificationEvaluator.evaluate(predictions)
      println(s"Accuracy of the test set is $accuracy")

      val t2 = System.nanoTime
      println(s"Applied the arrest prediction model in ${(t2 - t1) * 1E-9} seconds")

    } finally {
      spark.stop()
    }
  }
}
