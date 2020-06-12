import java.io.{File, FileWriter}
import java.util.{Properties, Timer, TimerTask, UUID}

import com.bingocloud.{ClientConfiguration, Protocol}
import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.nlpcn.commons.lang.util.IOUtil
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Main {
  val accessKey = ""
  val secretKey = ""
  val endpoint = "scuts3.depts.bingosoft.net:29999"
  val bucket = ""
  val key = "daas.txt"
  val topic = ""
  val prefix = ""
  val period = 5000
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"


  def main(args: Array[String]): Unit = {
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    val amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)
    val s3Object = amazonS3.getObject(bucket, key)
    val s3Content = IOUtil.getContent(s3Object.getObjectContent, "UTF-8")

    val props = new Properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val dataArr = s3Content.split("\n")
    for (s <- dataArr) {
      if (!s.trim.isEmpty) {
        val record = new ProducerRecord[String, String](topic, null, s)
        println("开始生产数据：" + s)
        producer.send(record)
      }
    }
    producer.flush()
    producer.close()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[ObjectNode](topic,
      new JSONKeyValueDeserializationSchema(false), kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)

    val inputKafkaStream = env.addSource(kafkaConsumer)
    inputKafkaStream.map(x=>(x.get("value").get("destination").asText, x.get("value").toString))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
        .process(new ProcessWindowFunction[(String, String), String, String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, String)], out: Collector[String]): Unit = {
            var result = ""
            for(e <- elements){
              result += e._2 + "\n"
            }
            out.collect(result)
          }
        })
        .writeUsingOutputFormat(new OutputFormat[String] {
          var timer: Timer = _
          var file: File = _
          var fileWriter: FileWriter = _
          var length = 0L
          var amazonS3: AmazonS3Client = _

          def upload: Unit = {
            this.synchronized {
              if (length > 0) {
                fileWriter.close()
                val targetKey = prefix+System.nanoTime()
                amazonS3.putObject(bucket, targetKey, file)
                println("开始上传文件：%s 至 %s 桶的 %s 目录下".format(file.getAbsoluteFile, bucket, targetKey))
                file = null
                fileWriter = null
                length = 0L
              }
            }
          }

          override def configure(configuration: Configuration): Unit = {
            timer = new Timer("S3Writer")
            timer.schedule(new TimerTask() {
              def run(): Unit = {
                upload
              }
            }, 1000, period)
            val credentials = new BasicAWSCredentials(accessKey, secretKey)
            val clientConfig = new ClientConfiguration()
            clientConfig.setProtocol(Protocol.HTTP)
            amazonS3 = new AmazonS3Client(credentials, clientConfig)
            amazonS3.setEndpoint(endpoint)

          }

          override def open(taskNumber: Int, numTasks: Int): Unit = {

          }

          override def writeRecord(it: String): Unit = {
            this.synchronized {
              if (StringUtils.isNoneBlank(it)) {
                if (fileWriter == null) {
                  file = new File(System.nanoTime() + ".txt")
                  fileWriter = new FileWriter(file, true)
                }
                fileWriter.append(it + "\n")
                length += it.length
                fileWriter.flush()
              }
            }
          }


          override def close(): Unit = {
            fileWriter.flush()
            fileWriter.close()
            timer.cancel()
          }
        })
    env.execute()
  }

}

