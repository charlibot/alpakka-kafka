/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import java.util.Locale

import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.TransactionalMessage
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{AutoSubscription, ConsumerSettings, Subscription}
import akka.stream.SourceShape
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.IsolationLevel

/** Internal API */
@InternalApi
private[kafka] final class TransactionalSource[K, V](consumerSettings: ConsumerSettings[K, V],
                                                     subscription: Subscription)
    extends KafkaSourceStage[K, V, TransactionalMessage[K, V]](
      s"TransactionalSource ${subscription.renderStageAttribute}"
    ) {
  require(consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty, "You must define a Consumer group.id.")

  /**
   * We set the isolation.level config to read_committed to make sure that any consumed messages are from
   * committed transactions. Note that the consuming partitions may be produced by multiple producers, and these
   * producers may either use transactional messaging or not at all. So the fetching partitions may have both
   * transactional and non-transactional messages, and by setting isolation.level config to read_committed consumers
   * will still consume non-transactional messages.
   */
  private val txConsumerSettings = consumerSettings.withProperty(
    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
    IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH)
  )

  override protected def logic(shape: SourceShape[TransactionalMessage[K, V]]): GraphStageLogic with Control =
    new SingleSourceLogic[K, V, TransactionalMessage[K, V]](shape, txConsumerSettings, subscription)
    with TransactionalMessageBuilder[K, V] {
      override def groupId: String = txConsumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG)
    }
}

/** Internal API */
@InternalApi
private[kafka] final class TransactionalSubSource[K, V](consumerSettings: ConsumerSettings[K, V],
                                                        subscription: AutoSubscription)
    extends KafkaSourceStage[K, V, (TopicPartition, Source[TransactionalMessage[K, V], Control])](
      s"TransactionalSubSource ${subscription.renderStageAttribute}"
    ) {
  require(consumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG).nonEmpty, "You must define a Consumer group.id.")

  /**
   * We set the isolation.level config to read_committed to make sure that any consumed messages are from
   * committed transactions. Note that the consuming partitions may be produced by multiple producers, and these
   * producers may either use transactional messaging or not at all. So the fetching partitions may have both
   * transactional and non-transactional messages, and by setting isolation.level config to read_committed consumers
   * will still consume non-transactional messages.
   */
  private val txConsumerSettings = consumerSettings.withProperty(
    ConsumerConfig.ISOLATION_LEVEL_CONFIG,
    IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ENGLISH)
  )

  override protected def logic(
      shape: SourceShape[(TopicPartition, Source[TransactionalMessage[K, V], Control])]
  ): GraphStageLogic with Control =
    new SubSourceLogic[K, V, TransactionalMessage[K, V]](shape, txConsumerSettings, subscription)
    with TransactionalMessageBuilder[K, V] {
      override def groupId: String = txConsumerSettings.properties(ConsumerConfig.GROUP_ID_CONFIG)
    }
}
