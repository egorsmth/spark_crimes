import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{broadcast, col, collect_list, concat_ws, lit, percentile_approx, sum, udf}

object BostonTest {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("<input_file_1> <input_file_2> <output_path>")
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

    val top_types = spark.sql("select district, offense_type, row_number() OVER (PARTITION BY district ORDER BY count(incident_number) DESC) rank from crimes group by district, CRIME_TYPE")
      .where("rank < 4")
      .groupBy("district")
      .agg(concat_ws(", ", collect_list("OFFENSE_TYPE")).alias("top_types"))
      .select("district", "top_types")

    val mean_by_month = spark.sql("select district, year, month, count(incident_number) as incidents_count from crimes where district is not null group by district, year, month order by district")
      .groupBy("district")
      .agg(percentile_approx(col("incidents_count"), lit(0.5), lit(100)).alias("median_incidents_count"))
      .select("district", "median_incidents_count")

    val common = spark.sql("select district, count(incident_number) as total_incidents, avg(long) as avg_long, avg(lat) as avg_lat from crimes group by district")
      .join(top_types, "district")
      .join(mean_by_month, "district")

    common.show(10)

    common.write.parquet(s"${output}/result.parquet")

//    spark.stop()

  }
}
