package edu.ucr.cs.cs167.group11

import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

object BeastScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Beast Crime Data Preparation")
    if (!conf.contains("spark.master")) conf.setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val spark = sparkSession
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    if (args.length < 4) {
      Console.err.println("Usage: <operation> <inputFile> <startDate> <endDate>")
      sys.exit(1)
    }
    val operation: String = args(0)
    val inputFile: String = args(1)

    try {
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "Temporal" =>
          val crimesDF: DataFrame = spark.read.parquet(inputFile)
          crimesDF.createOrReplaceTempView("crimes")

          val startDate = args(2)
          val endDate = args(3)
          val crimeTypeCount: DataFrame = spark.sql(
            s"""
               SELECT PrimaryType, COUNT(*) AS count
               FROM (
                 SELECT *, to_timestamp(Date, 'MM/dd/yyyy hh:mm:ss a') AS crime_ts
                 FROM crimes
               ) t
               WHERE to_date(crime_ts, 'MM/dd/yyyy') BETWEEN to_date('$startDate', 'MM/dd/yyyy')
                     AND to_date('$endDate', 'MM/dd/yyyy')
               GROUP BY PrimaryType
               ORDER BY count DESC
               """.stripMargin)

          val outputFile = s"CrimeTypeCount_${startDate.replace("/", "-")}_to_${endDate.replace("/", "-")}"
          crimeTypeCount.coalesce(1)
            .write.mode(SaveMode.Overwrite)
            .option("header", "true")
            .csv(outputFile)

          val outputDir = new File(outputFile)
          val partFiles = outputDir.listFiles().filter(_.getName.startsWith("part"))

          val partFile = partFiles.head
          val newFile = new File(outputDir, "CrimeTypeCount.csv")
          partFile.renameTo(newFile)



          crimeTypeCount.printSchema()





        case _ =>
          validOperation = false
      }

      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")

    } finally {
      sparkSession.stop()
    }
  }
}