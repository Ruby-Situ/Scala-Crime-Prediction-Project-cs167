package edu.ucr.cs.cs167.group11
/*
Project A, Task 4

Command-line arguments:
0 - input parquet filename, String
1 - start date of filter, String MM/DD/YYYY
2 - end date of filter, String MM/DD/YYYY
3 - x min for filter, Float
4 - y min for filter, Float
5 - x max for filter, Float
6 - y max for filter, Float
*/
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object CrimeRangeReport {

  def main(args: Array[String]) {
    println("arguments = " + args.mkString(" "))

    // Parse command-line arguments
    val input_file: String = args(0)
    val start_date: String = args(1)
    val end_date: String = args(2)
    val x_min: Float = args(3).toFloat
    val y_min: Float = args(4).toFloat
    val x_max: Float = args(5).toFloat
    val y_max: Float = args(6).toFloat

    // Output file name as specified in requirements
    val output_path: String = "RangeReportResult.csv"

    // Configure Spark
    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    // Create Spark session
    val spark = SparkSession
      .builder()
      .appName("CS167 Project - Task 4")
      .config(conf)
      .getOrCreate()

    try {
      // Load parquet file
      var df: DataFrame = spark.read.parquet(input_file)
      df.createOrReplaceTempView("crimes")

      // Run SQL query that does the filtering
      df = spark.sql(
        s"""
        SELECT *
        FROM (
          SELECT *, to_timestamp(Date, 'MM/dd/yyyy hh:mm:ss a') as timestamp
          FROM crimes
        ) temp
        WHERE timestamp BETWEEN
          to_date('$start_date', 'MM/dd/yyyy') AND
          to_date('$end_date', 'MM/dd/yyyy')
          AND x BETWEEN $x_min AND $x_max
          AND y BETWEEN $y_min AND $y_max
        """
      )

      // Select only required columns for output
      df = df.select("x", "y", "CaseNumber", "Date")

      // Show sample of results
      df.show()

      // Write output to CSV
      df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    }
    finally {
      spark.stop
    }
  }
}