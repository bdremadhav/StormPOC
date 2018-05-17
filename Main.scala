package com.madhav.poc

import java.util.Properties

import org.apache.storm.kafka.spout.{KafkaSpout, KafkaSpoutConfig}
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.{Config, LocalCluster}

object Main{

  final val BOOTSTRAP_SERVERS:String = "localhost:9092"
  final val TOPIC_NAME:String = "test2"

  def main(args: Array[String]):Unit = {
    val props: Properties = new Properties
    props.put("bootstrap.servers", BOOTSTRAP_SERVERS)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaSpoutConfig: KafkaSpoutConfig[String, String] = KafkaSpoutConfig.builder(BOOTSTRAP_SERVERS, TOPIC_NAME).setGroupId("CG2").setProp(props).build

    val kafkaSpout: KafkaSpout[String, String] = new KafkaSpout[String, String](kafkaSpoutConfig)

    val builder: TopologyBuilder = new TopologyBuilder
    builder.setSpout("kafka_spout", kafkaSpout)
    builder.setBolt("uppercase_bolt", new UpperCaseBolt).shuffleGrouping("kafka_spout")

    val conf: Config = new Config
    conf.setDebug(true)
    val cluster: LocalCluster = new LocalCluster
    cluster.submitTopology("test-kafka", conf, builder.createTopology)

  }
}
