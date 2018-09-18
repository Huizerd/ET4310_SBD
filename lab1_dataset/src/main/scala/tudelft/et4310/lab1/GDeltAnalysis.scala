package tudelft.et4310.lab1

import java.sql.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object GDeltAnalysis {

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
      .appName("GDeltAnalysis")
      .master("local[8]") // use 8 cores
      .getOrCreate()
    val sc = spark.sparkContext

    // For implicit conversions
    import spark.implicits._

    // Import data (lab1_dataset is root folder)
    val ds = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .option("dateFormat", "yyyyMMddhhmmss")
      .csv("/home/huis/Projects/ET4310_SBD/data/segment/*.csv")
      .select("publishDate", "allNames")
      .as[RawData] // to dataset (instead of dataframe)

    // Remove rows with nulls and convert back to dataset
    val ds_nonull = ds.na.drop().as[RawData]

    // Map to tuples with counts
    val processed = ds_nonull.map(row =>
      ProcessedData(row.publishDate, row.allNames.split(";").map(name => (name.split(",")(0), 1))))

    // Reduce (how?)
    // val processed_reduce = processed_map.reduce()

    // Show results
    processed.show()

    spark.stop
  }

  // Classes for DataSet
  case class RawData(
                      publishDate: Date,
                      allNames: String
                    )

  case class ProcessedData(
                            publishDate: Date,
                            allNames: Array[(String, Int)]
                          )

}
