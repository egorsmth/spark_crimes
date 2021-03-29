package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BostonCrimesMap {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("USAGE: <input Crimes csv> <input codes csv> <output_path>")
      scala.sys.exit(1)
    }

    val crimeFile = args(0)
    val codesFile = args(1)
    val output = args(2)

    val spark: SparkSession = SparkSession.builder().appName("BostonTest").getOrCreate()

    val toOffType = udf((v: String) => v.split(" - ").head)
    spark.udf.register("toOffType", toOffType)

    val codes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(codesFile)
      .dropDuplicates("code")
      .where("CODE is not NULL")

    val crimes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(crimeFile)
      .where("OFFENSE_CODE is not NULL AND DISTRICT is not NULL AND YEAR is not NULL AND MONTH is not NULL")
      .join(broadcast(codes))
      .where("CODE = OFFENSE_CODE")
      .withColumn("CRIME_TYPE", toOffType(codes.col("NAME")).alias("CRIME_TYPE"))
      .select("INCIDENT_NUMBER", "DISTRICT", "YEAR", "MONTH", "lat", "long", "CRIME_TYPE")

    crimes.createOrReplaceTempView("crimes")

    val top_types = spark.sql("select " +
      "DISTRICT, CRIME_TYPE, row_number() OVER (PARTITION BY district ORDER BY count(INCIDENT_NUMBER) DESC) RANK " +
      "from crimes " +
      "group by DISTRICT, CRIME_TYPE")
      .where("RANK < 4")
      .groupBy("DISTRICT")
      .agg(concat_ws(", ", collect_list("CRIME_TYPE")).alias("frequent_crime_types"))
      .select("DISTRICT", "frequent_crime_types")

    val mean_by_month = spark.sql("select " +
      "DISTRICT, year, month, count(INCIDENT_NUMBER) as incidents_count " +
      "from crimes " +
      "where district is not null " +
      "group by DISTRICT, year, month " +
      "order by DISTRICT")

      .groupBy("DISTRICT")
      .agg(percentile_approx(col("incidents_count"), lit(0.5), lit(100)).alias("crimes_monthly"))
      .select("DISTRICT", "crimes_monthly")

    val common = spark.sql("select " +
      "DISTRICT, count(INCIDENT_NUMBER) as total_incidents, avg(long) as lng, avg(lat) as lat " +
      "from crimes " +
      "group by DISTRICT")
      .join(top_types, "DISTRICT")
      .join(mean_by_month, "DISTRICT")

    common.write.parquet(output)

    spark.stop()

  }
}
