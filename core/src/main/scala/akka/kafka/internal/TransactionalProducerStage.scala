/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal
import akka.Done
import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage
import akka.kafka.ConsumerMessage.{GroupTopicPartition, PartitionOffset}
import akka.kafka.ProducerMessage.{Envelope, Results}
import akka.kafka.internal.ProducerStage.{MessageCallback, ProducerCompletionState}
import akka.stream.{Attributes, FlowShape}
import akka.stream.stage._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.TopicPartition

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.collection.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[kafka] final class TransactionalProducerStage[K, V, P](
    val closeTimeout: FiniteDuration,
    val closeProducerOnStop: Boolean,
    val producerProvider: String => Producer[K, V],
    transactionalId: Option[String],
    commitInterval: FiniteDuration,
    streamCompletePromise: Promise[Done]
) extends GraphStage[FlowShape[Envelope[K, V, P], Future[Results[K, V, P]]]]
    with ProducerStage[K, V, P, Envelope[K, V, P], Results[K, V, P], String] {

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TransactionalProducerStageLogic(this,
                                        producerProvider,
                                        transactionalId,
                                        inheritedAttributes,
                                        commitInterval,
                                        streamCompletePromise)
}

/** Internal API */
private object TransactionalProducerStage {
  object TransactionBatch {
    def empty: TransactionBatch = new EmptyTransactionBatch()
  }

  sealed trait TransactionBatch {
    def updated(partitionOffset: PartitionOffset): TransactionBatch
  }

  final class EmptyTransactionBatch extends TransactionBatch {
    override def updated(partitionOffset: PartitionOffset): TransactionBatch =
      new NonemptyTransactionBatch(partitionOffset)
  }

  final class NonemptyTransactionBatch(head: PartitionOffset,
                                       tail: Map[GroupTopicPartition, Long] = Map[GroupTopicPartition, Long]())
      extends TransactionBatch {
    private val offsets = tail + (head.key -> head.offset)

    def group: String = head.key.groupId
    def offsetMap(): Map[TopicPartition, OffsetAndMetadata] = offsets.map {
      case (gtp, offset) => new TopicPartition(gtp.topic, gtp.partition) -> new OffsetAndMetadata(offset + 1)
    }

    override def updated(partitionOffset: PartitionOffset): TransactionBatch = {
      require(
        group == partitionOffset.key.groupId,
        s"Transaction batch must contain messages from exactly 1 consumer group. $group != ${partitionOffset.key.groupId}"
      )
      new NonemptyTransactionBatch(partitionOffset, offsets)
    }
  }

}

/**
 * Internal API.
 *
 * Transaction (Exactly-Once) Producer State Logic
 */
private final class TransactionalProducerStageLogic[K, V, P](stage: TransactionalProducerStage[K, V, P],
                                                             producerProvider: String => Producer[K, V],
                                                             transactionalId: Option[String],
                                                             inheritedAttributes: Attributes,
                                                             commitInterval: FiniteDuration,
                                                             streamCompletePromise: Promise[Done])
    extends DefaultProducerStageLogic[K, V, P, Envelope[K, V, P], Results[K, V, P], String](stage,
                                                                                            producerProvider,
                                                                                            inheritedAttributes)
    with StageLogging
    with MessageCallback[K, V, P]
    with ProducerCompletionState {

  override protected var producer: Producer[K, V] = _
  private var initiated: Boolean = _

  import TransactionalProducerStage._

  private val commitSchedulerKey = "commit"
  private val messageDrainInterval = 10.milliseconds

  private var batchOffsets = TransactionBatch.empty

  override def preStart(): Unit = {
    transactionalId.fold {
      initiated = false
    } { txid =>
      producer = producerProvider(txid)
      initTransactions()
      beginTransaction()
    }
    resumeDemand(tryToPull = false)
    scheduleOnce(commitSchedulerKey, commitInterval)
  }

  private def resumeDemand(tryToPull: Boolean = true): Unit = {
    setHandler(stage.out, new OutHandler {
      override def onPull(): Unit = tryPull(stage.in)
    })
    // kick off demand for more messages if we're resuming demand
    if (tryToPull && isAvailable(stage.out) && !hasBeenPulled(stage.in)) {
      tryPull(stage.in)
    }
  }

  private def suspendDemand(): Unit =
    setHandler(
      stage.out,
      new OutHandler {
        // suspend demand while a commit is in process so we can drain any outstanding message acknowledgements
        override def onPull(): Unit = ()
      }
    )

  override protected def onTimer(timerKey: Any): Unit =
    if (timerKey == commitSchedulerKey) {
      maybeCommitTransaction()
    }

  private def maybeCommitTransaction(beginNewTransaction: Boolean = true): Unit = {
    val awaitingConf = awaitingConfirmation.get
    batchOffsets match {
      case batch: NonemptyTransactionBatch if awaitingConf == 0 =>
        commitTransaction(batch, beginNewTransaction)
      case _ if awaitingConf > 0 =>
        suspendDemand()
        scheduleOnce(commitSchedulerKey, messageDrainInterval)
      case _ =>
        scheduleOnce(commitSchedulerKey, commitInterval)
    }
  }

  override def produce(in: Envelope[K, V, P]): Unit = {
    if (!initiated) {
      in.passThrough match {
        case ConsumerMessage.PartitionOffset(gtp, _) =>
          producer = producerProvider(gtp.groupId + "-" + gtp.topic + "-" + gtp.partition)
          initTransactions()
          beginTransaction()
        case _ =>
          failStage(new IllegalStateException("Transactional producer passthrough does not contain the partition info"))
      }
      initiated = true
    }
    super.produce(in)
  }

  override val onMessageAckCb: AsyncCallback[Envelope[K, V, P]] =
    getAsyncCallback[Envelope[K, V, P]](_.passThrough match {
      case o: ConsumerMessage.PartitionOffset => batchOffsets = batchOffsets.updated(o)
      case _ =>
    })

  override def onCompletionSuccess(): Unit = {
    log.debug("Committing final transaction before shutdown")
    cancelTimer(commitSchedulerKey)
    maybeCommitTransaction(beginNewTransaction = false)
    streamCompletePromise.success(Done)
    super.onCompletionSuccess()
  }

  override def onCompletionFailure(ex: Throwable): Unit = {
    log.debug("Aborting transaction due to stage failure")
    abortTransaction()
    streamCompletePromise.failure(ex)
    super.onCompletionFailure(ex)
  }

  private def commitTransaction(batch: NonemptyTransactionBatch, beginNewTransaction: Boolean): Unit = {
    val group = batch.group
    log.debug("Committing transaction for consumer group '{}' with offsets: {}", group, batch.offsetMap())
    val offsetMap = batch.offsetMap().asJava
    producer.sendOffsetsToTransaction(offsetMap, group)
    producer.commitTransaction()
    batchOffsets = TransactionBatch.empty
    if (beginNewTransaction) {
      beginTransaction()
      resumeDemand()
      scheduleOnce(commitSchedulerKey, commitInterval)
    }
  }

  private def initTransactions(): Unit = {
    log.debug("Initializing transactions")
    producer.initTransactions()
  }

  private def beginTransaction(): Unit = {
    log.debug("Beginning new transaction")
    producer.beginTransaction()
  }

  private def abortTransaction(): Unit = {
    log.debug("Aborting transaction")
    producer.abortTransaction()
  }
}
