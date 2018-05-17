package com.madhav.poc

import java.util

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.{IRichBolt, OutputFieldsDeclarer}
import org.apache.storm.tuple.{Fields, Tuple, Values}
/**
  * Created by cloudera on 5/16/18.
  */
class UpperCaseBolt extends IRichBolt {
   var coll: OutputCollector = null

  def prepare(conf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    coll = collector
  }

  def execute(tuple: Tuple) {
    System.out.println("tuple = " + tuple)
    coll.emit(tuple, new Values(tuple.getString(3).toUpperCase))
    coll.ack(tuple)
  }

  def cleanup {
  }

  def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("word"))
  }

  def getComponentConfiguration: util.Map[String, AnyRef] = {
    return null
  }
}
