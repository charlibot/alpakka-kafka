/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.scaladsl

import akka.kafka.ConsumerMessage.TransactionalMessage
import akka.kafka.ProducerMessage._
import akka.kafka.internal.{TransactionalProducerStage, TransactionalSource, TransactionalSubSource}
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka._
import akka.stream.ActorAttributes
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.{Done, NotUsed}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{Future, Promise}

/**
 * Akka Stream connector to support transactions between Kafka topics.
 */
object Transactional {

  /**
   * Transactional source to setup a stream for Exactly Only Once (EoS) kafka message semantics.  To enable EoS it's
   * necessary to use the [[Transactional.sink]] or [[Transactional.flow]] (for passthrough).
   */
  def source[K, V](settings: ConsumerSettings[K, V],
                   subscription: Subscription): Source[TransactionalMessage[K, V], Control] =
    Source.fromGraph(new TransactionalSource[K, V](settings, subscription))

  /**
   * The `partitionedSource` is a way to track automatic partition assignment from kafka.
   * When a topic-partition is assigned to a consumer, this source will emit tuples with the assigned topic-partition,
   * a promise that should be passed to the [[Transactional.sink]] or [[Transactional.flow]] and a corresponding
   * source of `TransactionalMessage`s. Each source is setup for for Exactly Only Once (EoS) kafka message semantics.
   * To enable EoS it's necessary to use the [[Transactional.sink]] or [[Transactional.flow]] (for passthrough).
   * When Kafka rebalances partitions, all sources complete before the remaining sources are issued again.
   *
   * By generating the `transactionalId` from the [[TopicPartition]], multiple instances of your application can run
   * without having to manually assign partitions to each instance.
   */
  def partitionedSource[K, V](
      settings: ConsumerSettings[K, V],
      subscription: AutoSubscription
  ): Source[(TopicPartition, Promise[Done], Source[TransactionalMessage[K, V], NotUsed]), Control] =
    Source.fromGraph(new TransactionalSubSource[K, V](settings, subscription))

  /**
   * Sink that is aware of the [[ConsumerMessage.TransactionalMessage.partitionOffset]] from a [[Transactional.source]].  It will
   * initialize, begin, produce, and commit the consumer offset as part of a transaction.
   */
  def sink[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Sink[Envelope[K, V, ConsumerMessage.PartitionOffset], Future[Done]] =
    sink(settings, transactionalId, Promise())

  /**
   * Sink that is aware of the [[ConsumerMessage.TransactionalMessage.partitionOffset]] from a [[Transactional.source]].  It will
   * initialize, begin, produce, and commit the consumer offset as part of a transaction.
   *
   * This sink requires a [[Promise]] that should originate from the [[partitionedSource]] in order to safely rebalance.
   */
  def sink[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String,
      streamCompletePromise: Promise[Done]
  ): Sink[Envelope[K, V, ConsumerMessage.PartitionOffset], Future[Done]] =
    flow(settings, transactionalId, streamCompletePromise).toMat(Sink.ignore)(Keep.right)

  def sinkFromPartitioned[K, V](
      settings: ProducerSettings[K, V],
      streamCompletePromise: Promise[Done] = Promise()
  ): Sink[Envelope[K, V, ConsumerMessage.PartitionOffset], Future[Done]] =
    flowFromPartitioned(settings, streamCompletePromise).toMat(Sink.ignore)(Keep.right)

  def flowFromPartitioned[K, V](
      settings: ProducerSettings[K, V],
      streamCompletePromise: Promise[Done] = Promise()
  ): Flow[Envelope[K, V, ConsumerMessage.PartitionOffset], Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] = {
    flowCommon(settings, None, streamCompletePromise)
  }

  private def flowCommon[K, V](
    settings: ProducerSettings[K, V],
    transactionalId: Option[String],
    streamCompletePromise: Promise[Done] = Promise()
  ): Flow[Envelope[K, V, ConsumerMessage.PartitionOffset], Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] = {
    val txSettingsToProducer = (txid: String) => {
      settings
        .withProperties(
          ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
          ProducerConfig.TRANSACTIONAL_ID_CONFIG -> txid,
          ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> 1.toString
        )
        .createKafkaProducer()
    }

    val flow = Flow
      .fromGraph(
        new TransactionalProducerStage[K, V, ConsumerMessage.PartitionOffset](
          settings.closeTimeout,
          closeProducerOnStop = true,
          txSettingsToProducer,
          transactionalId,
          settings.eosCommitInterval,
          streamCompletePromise
        )
      )
      .mapAsync(settings.parallelism)(identity)

    flowWithDispatcher(settings, flow)
  }

  /**
   * Publish records to Kafka topics and then continue the flow.  The flow should only used with a [[Transactional.source]] that
   * emits a [[ConsumerMessage.TransactionalMessage]].  The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly once transactional support.
   */
  def flow[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String
  ): Flow[Envelope[K, V, ConsumerMessage.PartitionOffset], Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] =
    flow(settings, transactionalId, Promise())

  /**
   * Publish records to Kafka topics and then continue the flow.  The flow should only used with a [[Transactional.source]] that
   * emits a [[ConsumerMessage.TransactionalMessage]].  The flow requires a unique `transactional.id` across all app
   * instances.  The flow will override producer properties to enable Kafka exactly once transactional support.
   *
   * This flow requires a [[Promise]] that should originate from the [[partitionedSource]] in order to safely rebalance.
   */
  def flow[K, V](
      settings: ProducerSettings[K, V],
      transactionalId: String,
      streamCompletePromise: Promise[Done]
  ): Flow[Envelope[K, V, ConsumerMessage.PartitionOffset], Results[K, V, ConsumerMessage.PartitionOffset], NotUsed] = {
    require(transactionalId != null && transactionalId.length > 0, "You must define a Transactional id.")
    flowCommon(settings, Some(transactionalId), streamCompletePromise)
  }

  private def flowWithDispatcher[PassThrough, V, K](
      settings: ProducerSettings[K, V],
      flow: Flow[Envelope[K, V, PassThrough], Results[K, V, PassThrough], NotUsed]
  ) =
    if (settings.dispatcher.isEmpty) flow
    else flow.withAttributes(ActorAttributes.dispatcher(settings.dispatcher))
}
