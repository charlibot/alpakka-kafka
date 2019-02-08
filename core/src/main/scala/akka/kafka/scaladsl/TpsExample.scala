/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.ActorSystem
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.Transactional.TopicPartitionAndControl
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object TpsExample {

  implicit val actorSystem = ActorSystem()
  implicit val ec = actorSystem.dispatcher
  implicit val actorMaterializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {
    val consumerSettings = ConsumerSettings(actorSystem, new StringDeserializer, new StringDeserializer)
    val topic = "in"
    val outputTopic = "out"
    val maxPartitions = 4

    val producerSettings = ProducerSettings(actorSystem, new StringSerializer, new StringSerializer)
    val subscription = Subscriptions.topics(topic)

    val (rebalanceRef, partitionedSource) = Transactional.partitionedSource(consumerSettings, subscription)
    val control = partitionedSource
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
          rebalanceRef ! TopicPartitionAndControl(tp, drainingControl)
          drainingControl.streamCompletion
      }
      .toMat(Sink.ignore)(Keep.both)
      .mapMaterializedValue(DrainingControl.apply)
      .run()
  }

}
