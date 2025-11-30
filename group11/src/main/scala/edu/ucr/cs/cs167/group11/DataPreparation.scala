package edu.ucr.cs.cs167.group11

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object DataPreparation {

  def main(args: Array[String]): Unit = {

    // Spark Configuration
    //setting up configuration and master settings local too
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    // staring spark session
    val spark = SparkSession
      .builder()
      .appName("DataPreparation")
      .config(conf)
      .getOrCreate()

    // custom UDTs and UDFs for spatial data handling in SparkSQL
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession = spark)
    val sparkContext = spark.sparkContext

    // Input and output paths
    val inputfile: String = args(0)
    val outputFile: String = "Chicago_Crimes_ZIP"

    try {
      import edu.ucr.cs.bdlab.beast._

      // Read Chicago crime data from CSV file given
      val df = spark.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        // Ensure header is read correct
        .load(inputfile)

        //columns renamed
        .withColumnRenamed("Case Number", "CaseNumber")
        .withColumnRenamed("Primary Type", "PrimaryType")
        .withColumnRenamed("Location Description", "LocationDescription")
        .withColumnRenamed("Community Area", "CommunityArea")
        .withColumnRenamed("FBI Code", "FBICode")
        .withColumnRenamed("X Coordinate", "XCoordinate")
        .withColumnRenamed("Y Coordinate", "YCoordinate")
        .withColumnRenamed("FBI Code", "FBICode")
        .withColumnRenamed("Updated On", "UpdatedOn")

      //print the schema and display sample of the crime data
      df.printSchema()
      df.show()

      //SPATIAL ANALYSIS
      //Create a geometry column using X and Y coordinates (spatial analysis)
      val crimesRDD: SpatialRDD = df.selectExpr("*", "ST_CreatePoint(x, y) AS geometry").toSpatialRDD
      println("Geometry created. Loaded crime records ")

      //load zip code
      val zipsRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")
      println("Zip code shapefile loaded")

      // Perform spatial join between crime locations and ZIP codes, each crime associated with zipcode
      val crimeZipRDD: RDD[(IFeature, IFeature)] = crimesRDD.spatialJoin(zipsRDD)
      println("Spatial Join Complete .RDDs Joined")

      val crimeZip: DataFrame = crimeZipRDD.map({ case (geometry, zipcode) => Feature.append(geometry, zipcode.getAs[String]("ZCTA5CE10"), "ZIPCode") })
        .toDataFrame(spark)
        .drop("geometry")
      println("Final Dataframe Created")

      // final schema printed
      crimeZip.printSchema()
      crimeZip.write.mode(SaveMode.Overwrite).parquet(outputFile)

    }
      //stopping spark
    finally {
      spark.stop
    }

  }
}