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

  val keyValueStoreBuilder2 = Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore("timeStore"),
    Serdes.String,
    Serdes.Long
  )

  builder.addStateStore(keyValueStoreBuilder2)


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

  var r2 = records.transform(new HistogramTransformer(), "countStore","timeStore")
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
  var kvStoreTime: KeyValueStore[String, Long] = _
  var hourInMs = 3600*1000

  def init(context: ProcessorContext) {
    this.context = context
    this.kvStoreCount = context.getStateStore("countStore").asInstanceOf[KeyValueStore[String, Long]]
    this.kvStoreTime = context.getStateStore("timeStore").asInstanceOf[KeyValueStore[String, Long]]

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
  }
  def transform(key: String, name: String): (String, Long) = {
    val oldCount: Long = this.kvStoreCount.get(name)
    val existing = Option(oldCount)
    val currTime = context.timestamp

    if(existing == None){ //initialize
      this.kvStoreCount.put(name, 1)
      this.kvStoreTime.put(name, currTime)
    }else{
      val createTime = this.kvStoreTime.get(name)
      if(currTime-createTime>hourInMs){ // reset
        this.kvStoreCount.put(name, 1)
        this.kvStoreTime.put(name, currTime)
      }else{ // update
        this.kvStoreCount.put(name, oldCount+1)
      }
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
