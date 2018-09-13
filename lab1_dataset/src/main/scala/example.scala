package example

import java.sql.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types._


object ExampleSpark {

  case class RawData(
                   publishDate: Date,
                   allNames: String
                 )

  case class ProcessedData(
                         publishDate: Date,
                         allNames: Array[(String, Int)]
                       )

  def main(args: Array[String]) {
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

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession
      .builder
      .appName("GDELThist")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sparkContext // If you need SparkContext object

    import spark.implicits._

    val ds = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .option("dateFormat", "yyyyMMddhhmmss")
      .csv("/home/huis/Projects/ET4310_SBD/data/segment/*.csv")
      .select("publishDate", "allNames")
      .as[RawData]  // to dataset (instead of dataframe)

    // Remove rows with nulls and convert back to dataset
    val ds_nonull = ds.na.drop().as[RawData]

    // In some way: keep only the names, count them per object, and create a list of tuples out of them --> MapReduce!
    // val pattern = """([^\;\,]+),\d+""".r
    // val processed = ds.map(data => (data.publishDate, data.allNames))
    // val processed = ds.map(data => (data.publishDate, data.allNames.split(";").map(name => (name.split(",")(0), 1))))
    // val rdd = ds.rdd.map(data => (data.publishDate, data.allNames))
    // val rdd_processed = rdd.map{case (date, name) => (date, name.split(";"))}

    // Map to tuples with counts
    val processed = ds_nonull.map(row => ProcessedData(row.publishDate, row.allNames.split(";").map(name => (name.split(",")(0), 1))))

    // Reduce 
    // val processed_reduce = processed_map.reduce()

    // ds_nonull.show()
    processed.show()
    // processed.take(2).foreach(println)

    spark.stop
  }
}
