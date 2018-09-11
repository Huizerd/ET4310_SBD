package example

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import java.sql.{Date, Timestamp}


object ExampleSpark {
  case class Data (
    publishDate: Date,
    allNames: String
  )
  def main(args: Array[String]) {
    val schema =
      StructType(
        Array(
          StructField("gkgRecordId", StringType, nullable=true),
          StructField("publishDate", DateType, nullable=true),
          StructField("sourceCollectionIdentifier", IntegerType, nullable=true),
          StructField("sourceCommonName", StringType, nullable=true),
          StructField("documentIdentifier", StringType, nullable=true),
          StructField("counts", StringType, nullable=true),
          StructField("enhancedCounts", StringType, nullable=true),
          StructField("themes", StringType, nullable=true),
          StructField("enhancedThemes", StringType, nullable=true),
          StructField("locations", StringType, nullable=true),
          StructField("enhancedLocations", StringType, nullable=true),
          StructField("persons", StringType, nullable=true),
          StructField("enhancedPersons", StringType, nullable=true),
          StructField("organisations", StringType, nullable=true),
          StructField("enhancedOrganisations", StringType, nullable=true),
          StructField("tone", StringType, nullable=true),
          StructField("enhancedDates", StringType, nullable=true),
          StructField("gcams", StringType, nullable=true),
          StructField("sharingImage", StringType, nullable=true),
          StructField("relatedImages", StringType, nullable=true),
          StructField("socialImageEmbeds", StringType, nullable=true),
          StructField("socialVideoEmbeds", StringType, nullable=true),
          StructField("quotations", StringType, nullable=true),
          StructField("allNames", StringType, nullable=true),
          StructField("amounts", StringType, nullable=true),
          StructField("translationInfo", StringType, nullable=true),
          StructField("extrasXML", StringType, nullable=true)
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
                  .as[Data]

    ds.take(10).foreach(println)

    spark.stop
  }
}
