package lab3
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.state._
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
      .split(";", -1)
      .map(names => {
        val name = names.split(",")(0) // take only name, not offset
        (k, name)
      })
      .filter(x => x._2 != "" && x._2 != "Type ParentCategory") // filter for bad names
    })

  var r2 = records.transform(new HistogramTransformer(), "countStore", "timeStore")
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

// clear store after while, so not out of memory?
class HistogramTransformer extends Transformer[String, String, (String, Long)] {
  var context: ProcessorContext = _
  var countStore: KeyValueStore[String, Long] = _
  var timeStore: KeyValueStore[String, Long] = _
  val minInMs: Long = 60*1000
  var second: Long = _
  val secInMs: Long = 1000

  def init(context: ProcessorContext) {
    this.context = context
    this.countStore = context.getStateStore("countStore").asInstanceOf[KeyValueStore[String, Long]]
    this.timeStore = context.getStateStore("timeStore").asInstanceOf[KeyValueStore[String, Long]]
    this.second = 1 // first second from (initialization) is called second 1

    this.context.schedule(this.secInMs/10, PunctuationType.STREAM_TIME, (timestamp) => {
         val iter = this.countStore.all
         while (iter.hasNext) {
             val entry = iter.next()
             context.forward(entry.key, entry.value)
         }
         iter.close()
         context.commit()
     })

     // to avoid deleting input that shouldn't be deleted: delete those 59 back in time
     // alts: contex.timeStamp, System.nanoTime
     // problem: if get actual name that is called something like name60, and already have name before: solution custom serde, key = name, value = list of long's
     // this is slowing down stream? can still do transformations while this is schedule is running?
     // how long time is this taking? more than 1 sec => problem
     // is it working at all?

     this.context.schedule(this.secInMs, PunctuationType.WALL_CLOCK_TIME, (timestamp) =>{
        this.second = this.second + 1
        if(this.second >= 60){
          val iter = this.countStore.all
          var second = this.second%60 + 1 // delete those 59 sec back in time
          while(iter.hasNext){ // for every name
            var entry = iter.next()
            var name = entry.key
            var secName = name.concat(second.toString())
            var toBeDeleted = this.timeStore.get(secName)
            this.countStore.put(name, this.countStore.get(name) - toBeDeleted)
            this.timeStore.put(secName, 0)
          }
          iter.close()
          context.commit()
        }
     })
  }

  def transform(key: String, name: String): (String, Long) = {
    var oldCount: Long = this.countStore.get(name)
    var existing = Option(oldCount)
    var time = this.second%60 // 61 -> 1, 62 -> 2, and so on
    var secName = name.concat(time.toString()) // use this as key: name -> nameTime

    if(existing == None){
      this.countStore.put(name, 1)
      this.timeStore.put(secName, 1)
    }else{
      this.countStore.put(name, oldCount + 1)
      val existing2 = Option(this.timeStore.get(secName))
      if(existing2 == None){
        this.timeStore.put(secName, 1)
      }else{
        this.timeStore.put(secName, this.timeStore.get(secName) + 1)
      }
    }
    (name, this.countStore.get(name))
  }

  def punctuate(timestamp: Long){
    // do nothing
  }

  def close() {
    // do nothing
  }
}
