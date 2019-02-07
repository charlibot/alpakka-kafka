/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.pipe
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.TpsActor.TopicPartitionAndControl
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.Future

object TpsActor {

  case class TopicPartitionAndControl[T](drainingControl: DrainingControl[T], topicPartition: TopicPartition)

  def props(): Props = Props[TpsActor]

  implicit val actorSystem = ActorSystem()
  implicit val ec = actorSystem.dispatcher
  implicit val actorMaterializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseString("""
                                             |akka.kafka.committer.maxBatch = 1000
                                             |akka.kafka.committer.maxInterval = 10 seconds
                                             |akka.kafka.committer.parallelism = 1
                                             |
                                             |akka.kafka.consumer.wait-close-partition = 10 seconds
                                             |akka.kafka.consumer.kafka-clients {
                                             |  bootstrap.servers = "localhost:9092"
                                             |  group.id = "group-1"
                                             |  auto.offset.reset = "earliest"
                                             |}
                                           """.stripMargin)

    val consumerSettings = ConsumerSettings(actorSystem, new StringDeserializer, new StringDeserializer)
    val topic = "in"
    val outputTopic = "out"
    val maxPartitions = 4

    val producerSettings = ProducerSettings(actorSystem, new StringSerializer, new StringSerializer)
    val rebalanceRef = actorSystem.actorOf(props())
    val subscription = Subscriptions.topics(topic).withRebalanceListener(rebalanceRef)

    val control = Transactional
      .partitionedSource(consumerSettings, subscription)
      .mapAsyncUnordered(maxPartitions) {
        case (tp, source) =>
          val drainingControl = source
            .map { message =>
              ProducerMessage.single(new ProducerRecord[String, String](outputTopic, message.record.value() + "-out"),
                                     message.partitionOffset)
            }
            .toMat(Transactional.sink(producerSettings, s"tx-$tp"))(Keep.both)
            .mapMaterializedValue(DrainingControl.apply)
            .run()
          rebalanceRef ! TopicPartitionAndControl(drainingControl, tp)
          drainingControl.isShutdown
      }
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
  }

}

class TpsActor extends Actor {

  import context.dispatcher

  var topicPartitionAndFutures: Map[TopicPartition, DrainingControl[_]] = Map()

  override def receive: Receive = {
    case TopicPartitionsAssigned(subscription, assigned) =>
      sender ! akka.Done
    case TopicPartitionsRevoked(subscription, revoked) =>
      val client = sender()
      val completions = revoked.map { tp =>
        topicPartitionAndFutures(tp).drainAndShutdown()
      }
      Future.sequence(completions).map(_ => akka.Done) pipeTo client
    case TopicPartitionAndControl(drainingControl, topicPartition) =>
      topicPartitionAndFutures += topicPartition -> drainingControl
  }

}
