package lab3

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.{TimeWindows, Transformer}
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}


object GDELTStream extends App {

  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  // Filter this stream to a stream of (key, name). This is similar to Lab 1,
  // only without dates! After this apply the HistogramTransformer. Finally, 
  // write the result to a new topic called gdelt-histogram.
  // TODO: Ask Dorus: what do you mean, without dates?

  // Process incoming stream
  val records: KStream[String, Long] = builder
    .stream[String, String]("gdelt")
    .filter((k, v) => v.split("\t", -1).length == 27) // check correct length
    .flatMap((k, v) => {
    val columns = v.split("\t", -1)
    //    val newKey = columns(1).substring(0, 12) // take only yyyymmddhhMM
    columns(23)
      .split(";", -1)
      .map(names => {
        val name = names.split(",")(0) // take only name, not offset
        (name, 1L) // this long is not needed actually
      })
      .filter(x => x._1 != "" && x._1 != "Type ParentCategory") // filter for bad names
  })

  // Trying out windowed stream
  val windowed: TimeWindowedKStream[String, Long] = records
    .groupByKey // find something to just count names, so (name, 1) is not needed!
    .windowedBy(TimeWindows.of(3600000).advanceBy(60000)) // window of 1 hour, steps of 1 min

  // Stream to histogram
  windowed.count().toStream((k, v) => k.key()).to("gdelt-histogram")

  // To print keys/values, use this
  // It works when called here --> probably some problems with type conversion from Java <-> Scala
  //  windowed.foreach((k, v) => println(k + " - " + v))

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    println("Closing streams.")
    streams.close(10, TimeUnit.SECONDS)
  }

  System.in.read()
  System.exit(0)
}

// This transformer should count the number of times a name occurs 
// during the last hour. This means it needs to be able to 
//  1. Add a new record to the histogram and initialize its count;
//  2. Change the count for a record if it occurs again; and
//  3. Decrement the count of a record an hour later.
// You should implement the Histogram using a StateStore (see manual)
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _

  // Initialize Transformer object
  def init(context: ProcessorContext) {
    this.context = context
  }

  // Should return the current count of the name during the last hour
  def transform(key: String, name: String): (String, Long) = {
    ("Donald Trump", 1L)
  }

  // Close any resources if any
  def close() {
  }
}
