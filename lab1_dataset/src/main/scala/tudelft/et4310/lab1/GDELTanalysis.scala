package tudelft.et4310.lab1

import java.sql.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, explode, rank, split}
import org.apache.spark.sql.types._

object GDELTanalysis {

  def main(args: Array[String]) {

    // Define schema of files
    val schema =
      StructType(
        Array(
          StructField("gkgRecordId", StringType, nullable = false),
          StructField("publishDate", DateType, nullable = false),
          StructField("sourceCollectionIdentifier", IntegerType, nullable = false),
          StructField("sourceCommonName", StringType, nullable = false),
          StructField("documentIdentifier", StringType, nullable = false),
          StructField("counts", StringType, nullable = false),
          StructField("enhancedCounts", StringType, nullable = false),
          StructField("themes", StringType, nullable = false),
          StructField("enhancedThemes", StringType, nullable = false),
          StructField("locations", StringType, nullable = false),
          StructField("enhancedLocations", StringType, nullable = false),
          StructField("persons", StringType, nullable = false),
          StructField("enhancedPersons", StringType, nullable = false),
          StructField("organisations", StringType, nullable = false),
          StructField("enhancedOrganisations", StringType, nullable = false),
          StructField("tone", StringType, nullable = false),
          StructField("enhancedDates", StringType, nullable = false),
          StructField("gcams", StringType, nullable = false),
          StructField("sharingImage", StringType, nullable = false),
          StructField("relatedImages", StringType, nullable = false),
          StructField("socialImageEmbeds", StringType, nullable = false),
          StructField("socialVideoEmbeds", StringType, nullable = false),
          StructField("quotations", StringType, nullable = false),
          StructField("allNames", StringType, nullable = false),
          StructField("amounts", StringType, nullable = false),
          StructField("translationInfo", StringType, nullable = false),
          StructField("extrasXML", StringType, nullable = false)
        )
      )

    // Prevent too much console output
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // Get SparkSession and SparkContext
    val spark = SparkSession
      .builder
      .appName("GDELTanalysis")
      .master("local[8]") // use 8 cores
      .getOrCreate()
    val sc = spark.sparkContext

    // For implicit conversions
    import spark.implicits._

    // Import data (lab1_dataset is root folder)
    val data = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .option("dateFormat", "yyyyMMddhhmmss")
      .csv("/home/huis/Projects/ET4310_SBD/data/segment/*.csv")
      .select("publishDate", "allNames")
      .as[RawData] // to Dataset (instead of Dataframe)

    // Explode each name to separate row
    val explodedData = data
      .filter(_.allNames != null) // filter out empty names (date is always present)
      .withColumn("allNames", explode(split($"allNames", ";"))) // split names
      .withColumn("allNames", split($"allNames", ",")(0)).as[RawData] // remove char offsets

    // Reduce: count names per date
    val reducedData = explodedData
      .filter(_.allNames != "Type ParentCategory") // filter out this weird name
      .groupBy("publishDate", "allNames").count()
      .withColumnRenamed("allNames", "name").as[ProcessedData] // count names per date

    // Window definition for sorting (and limiting) --> why is there no sort within groups in Spark SQL??
    val w = Window.partitionBy("publishDate").orderBy(desc("count"))

    // Order per date, limit to 10
    val orderedData = reducedData.withColumn("rank", rank.over(w)).where($"rank" <= 10)
      .drop("rank").as[ProcessedData]

    // Show results
    orderedData.take(20).foreach(println)

    spark.stop
  }

  // Classes for DataSet
  case class RawData(
                      publishDate: Date,
                      allNames: String
                    )

  case class ProcessedData(
                            publishDate: Date,
                            name: String,
                            count: BigInt
                          )

}
