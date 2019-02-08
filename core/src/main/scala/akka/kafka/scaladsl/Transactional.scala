/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.pattern.ask
import akka.pattern.pipe
import akka.kafka.ConsumerMessage.TransactionalMessage
import akka.kafka.ProducerMessage._
import akka.kafka.internal.{TransactionalProducerStage, TransactionalSource, TransactionalSubSource}
import akka.kafka.scaladsl.Consumer.{Control, DrainingControl}
import akka.kafka._
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._
import scala.concurrent.Future

/**
 * Akka Stream connector to support transactions between Kafka topics.
 */
object Transactional {

  case class TopicPartitionAndControl[T](topicPartition: TopicPartition, control: DrainingControl[T])

  class RebalanceListener(original: Option[ActorRef]) extends Actor with ActorLogging {
    var topicPartitionAndControls: Map[TopicPartition, DrainingControl[_]] = Map()

    implicit val timeout
      : Timeout = Timeout(1 second) // timeout of asking the original actorRef and waiting for response
    import context.dispatcher
    override def receive: Receive = {
      case tpa @ TopicPartitionsAssigned(subscription, assigned) =>
        log.info(s"Received assigned: $assigned")
        val client = sender()
        askOriginal(tpa) pipeTo client
      case tpr @ TopicPartitionsRevoked(subscription, revoked) =>
        log.info(s"Received revoked: $revoked")
        val client = sender()
        askOriginal(tpr).flatMap { _ =>
          Future.sequence {
            revoked.flatMap(topicPartitionAndControls.get).map(_.drainAndShutdown())
          }
        } pipeTo client
      case TopicPartitionAndControl(topicPartition, control) =>
        log.info(s"Received topic partition $topicPartition")
        topicPartitionAndControls += topicPartition -> control
    }

    private def askOriginal[T](msg: T): Future[Done] =
      original.fold(Future.successful(Done))(_.ask(msg).map(_ => Done).recover {
        case e: Exception => Done
      })
  }

  /**
   * Transactional source to setup a stream for Exactly Only Once (EoS) kafka message semantics.  To enable EoS it's
   * necessary to use the [[Transactional.sink]] or [[Transactional.flow]] (for passthrough).
   */
  def source[K, V](settings: ConsumerSettings[K, V],
                   subscription: Subscription): Source[TransactionalMessage[K, V], Control] =
    Source.fromGraph(new TransactionalSource[K, V](settings, subscription))

  def partitionedSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription
  )(
      implicit system: ActorSystem
  ): (ActorRef, Source[(TopicPartition, Source[TransactionalMessage[K, V], Control]), Control]) = {
    val rebalanceListener = system.actorOf(Props(new RebalanceListener(subscription.rebalanceListener)))
    val source = Source.fromGraph(
      new TransactionalSubSource[K, V](settings, subscription.withRebalanceListener(rebalanceListener))
    )
    (rebalanceListener, source)
  }

  /**
   * Sink that is aware of the [[ConsumerMessage.TransactionalMessage.partitionOffset]] from a [[Transactional.source]].  It will
   * initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  def sink[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Sink[Envelope[K, V, ConsumerMessage.PartitionOffset], Future[Done]] =
    flow(settings, transactionalId).toMat(Sink.ignore)(Keep.right)

  /**
   * Publish records to Kafka topics and then continue the flow.  The flow should only used with a [[Transactional.source]] that
   * emits a [[ConsumerMessage.TransactionalMessage]].  The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly once transactional support.
   */
  def flow[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Flow[Envelope[K, V, ConsumerMessage.PartitionOffset], Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] = {
    require(transactionalId != null && transactionalId.length > 0, "You must define a Transactional id.")

    val txSettings = settings.withProperties(
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> transactionalId,
      ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString
    )

    val flow = Flow
      .fromGraph(
        new TransactionalProducerStage[K, V, ConsumerMessage.PartitionOffset](
          txSettings.closeTimeout,
          closeProducerOnStop = true,
          () => txSettings.createKafkaProducer(),
          settings.eosCommitInterval
        )
      )
      .mapAsync(txSettings.parallelism)(identity)

    flowWithDispatcher(txSettings, flow)
  }

  private def flowWithDispatcher[PassThrough, V, K](
      settings: ProducerSettings[K, V],
      flow: Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed]
  ) =
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
}
