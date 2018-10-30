package lab3
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}

object GDELTStream extends App {
  import Serdes._

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "lab3-gdelt-stream")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  val builder: StreamsBuilder = new StreamsBuilder

  val keyValueStoreBuilder = Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore("countStore"),
    Serdes.String,
    Serdes.Long
  )

  builder.addStateStore(keyValueStoreBuilder)

  val records: KStream[String, String] = builder.stream[String, String]("gdelt")
    .filter((k, v) => v.split("\t", -1).length == 27) // check correct length
    .flatMap((k,v) => {
    val columns = v.split("\t", -1)
    columns(23)
      .split(" ", -1)
      .map(names => {
        val name = names.split(",")(0) // take only name, not offset
        (k, name)
      })
      .filter(x => x._2 != "" && x._2 != "Type ParentCategory") // filter for bad names
    })

  var r2 = records.transform(new HistogramTransformer(), "countStore")
  r2.to("gdelt-histogram")

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

class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  var kvStoreCount: KeyValueStore[String, Long] = _
  var hourInMs = 3600*1000

  // must maybe clear the store, so not get out of memory
  def init(context: ProcessorContext) {
    this.context = context
    this.kvStoreCount = context.getStateStore("countStore").asInstanceOf[KeyValueStore[String, Long]]

    // send current result downStream every 1s
    this.context.schedule(1000, PunctuationType.STREAM_TIME, (timestamp) => {
         val iter = this.kvStoreCount.all
         while (iter.hasNext) {
             val entry = iter.next()
             context.forward(entry.key, entry.value)
         }
         iter.close()
         // commit the current processing progress
         context.commit()
     })

     // reset counts every 1h
     // problem: unclear how this schedule-method works.
     // problem: this assumes time starts when starts to run program
     // alt.: delete the store, and initialize it again (no mem. problem? possible?)
     this.context.schedule(hourInMs, PunctuationType.WALL_CLOCK_TIME, (timestamp)=>{
        val iter = this.kvStoreCount.all
        while(iter.hasNext) {
          val entry = iter.next()
          this.kvStoreCount.put(entry.key, 0)
        }
     })
  }

  def transform(key: String, name: String): (String, Long) = {
    val oldCount: Long = this.kvStoreCount.get(name)
    val existing = Option(oldCount)

    // alt.: one more kvStore storing the time each message came. Update this time
    //       when there has been more than 1 hour since last it came to transform.
    //       => problem: count isn't reset until next time key (msg). Should be res. earlier

    if(existing == None){ // initialize
      this.kvStoreCount.put(name, 1)
    }else{ // update
      this.kvStoreCount.put(name, oldCount+1)
    }
    (name, this.kvStoreCount.get(name))
  }

  def punctuate(timestamp: Long){
    // do nothing
  }

  def close() {
    // do nothing
  }
}
