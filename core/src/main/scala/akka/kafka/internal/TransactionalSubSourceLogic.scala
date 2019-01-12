/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.{Done, NotUsed}
import akka.actor.{ActorRef, ExtendedActorSystem, Status, Terminated}
import akka.annotation.InternalApi
import akka.kafka.Subscriptions.{TopicSubscription, TopicSubscriptionPattern}
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.pattern.{ask, AskTimeoutException}
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{StageLogging, _}
import akka.stream.{ActorMaterializerHelper, SourceShape}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

/**
 * Internal API.
 *
 * Anonymous sub-class instance is created in [[TransactionalSubSource]].
 */
@InternalApi
private abstract class TransactionalSubSourceLogic[K, V, Msg](
    val shape: SourceShape[(TopicPartition, Promise[Done], Source[Msg, NotUsed])],
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription
) extends GraphStageLogic(shape)
    with PromiseControl
    with MetricsControl
    with MessageBuilder[K, V, Msg]
    with StageLogging {

  val consumerPromise = Promise[ActorRef]
  final val actorNumber = KafkaConsumerActor.Internal.nextNumber()
  override def executionContext: ExecutionContext = materializer.executionContext
  override def consumerFuture: Future[ActorRef] = consumerPromise.future
  var consumerActor: ActorRef = _
  var sourceActor: StageActor = _

  /** Kafka has notified us that we have these partitions assigned, but we have not created a source for them yet. */
  var pendingPartitions: immutable.Set[TopicPartition] = immutable.Set.empty

  /** We have created a source for these partitions, but it has not started up and is not in subSources yet. */
  var partitionsInStartup: Map[TopicPartition, Future[Done]] = immutable.Map.empty
  var subSources: Map[TopicPartition, (Control, Future[Done])] = immutable.Map.empty

  override def preStart(): Unit = {
    super.preStart()

    sourceActor = getStageActor {
      case (_, Status.Failure(e)) =>
        failStage(e)
      case (_, Terminated(ref)) if ref == consumerActor =>
        failStage(new ConsumerFailed)
    }
    consumerActor = {
      val extendedActorSystem = ActorMaterializerHelper.downcast(materializer).system.asInstanceOf[ExtendedActorSystem]
      extendedActorSystem.systemActorOf(akka.kafka.KafkaConsumerActor.props(sourceActor.ref, settings),
                                        s"kafka-consumer-$actorNumber")
    }
    consumerPromise.success(consumerActor)
    sourceActor.watch(consumerActor)

    def rebalanceListener =
      KafkaConsumerActor.ListenerCallbacks(
        assignedTps => {
          subscription.rebalanceListener.foreach {
            _.tell(TopicPartitionsAssigned(subscription, assignedTps), sourceActor.ref)
          }
          if (assignedTps.nonEmpty) {
            partitionAssignedCB.invoke(assignedTps)
          }
        },
        revokedTps => {
          subscription.rebalanceListener.foreach {
            _.tell(TopicPartitionsRevoked(subscription, revokedTps), sourceActor.ref)
          }
          pendingPartitions --= revokedTps
          partitionsInStartup --= revokedTps
          implicit val ec: ExecutionContext = materializer.executionContext
          log.debug(s"Shutting down subsources for: $revokedTps")
          val futures = revokedTps
            .flatMap(subSources.get)
            .map { case (control, streamFuture) => control.shutdown().flatMap(_ => streamFuture) }
          Await.result(Future.sequence(futures), settings.transactionalStreamStopTimeout)
          log.debug(s"Finished shutting down subsources for: $revokedTps")
          subSources --= revokedTps
        }
      )

    subscription match {
      case TopicSubscription(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.Subscribe(topics, rebalanceListener), sourceActor.ref)
      case TopicSubscriptionPattern(topics, _) =>
        consumerActor.tell(KafkaConsumerActor.Internal.SubscribePattern(topics, rebalanceListener), sourceActor.ref)
    }
  }

  private val updatePendingPartitionsAndEmitSubSourcesCb =
    getAsyncCallback[Set[TopicPartition]](updatePendingPartitionsAndEmitSubSources)

  private val stageFailCB = getAsyncCallback[ConsumerFailed] { ex =>
    failStage(ex)
  }

  val partitionAssignedCB = getAsyncCallback[Set[TopicPartition]] { assigned =>
    updatePendingPartitionsAndEmitSubSources(assigned)
  }

  private def seekAndEmitSubSources(
      formerlyUnknown: Set[TopicPartition],
      offsets: Map[TopicPartition, Long]
  ): Unit = {
    implicit val ec: ExecutionContext = materializer.executionContext
    consumerActor
      .ask(KafkaConsumerActor.Internal.Seek(offsets))(Timeout(10.seconds), sourceActor.ref)
      .map(_ => updatePendingPartitionsAndEmitSubSourcesCb.invoke(formerlyUnknown))
      .recover {
        case _: AskTimeoutException =>
          stageFailCB.invoke(
            new ConsumerFailed(
              s"#$actorNumber Consumer failed during seek for partitions: ${offsets.keys.mkString(", ")}."
            )
          )
      }
  }

  val subsourceCancelledCB: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])] =
    getAsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])] {
      case (tp, firstUnconsumed) =>
        subSources -= tp
        partitionsInStartup -= tp
        pendingPartitions += tp
        firstUnconsumed match {
          case Some(record) =>
            if (log.isDebugEnabled) {
              log.debug("#{} Seeking {} to {} after partition SubSource cancelled", actorNumber, tp, record.offset())
            }
            seekAndEmitSubSources(formerlyUnknown = Set.empty, Map(tp -> record.offset()))
          case None => emitSubSourcesForPendingPartitions()
        }
    }

  val subsourceStartedCB: AsyncCallback[(TopicPartition, Control)] = getAsyncCallback[(TopicPartition, Control)] {
    case (tp, control) =>
      if (!partitionsInStartup.contains(tp)) {
        // Partition was revoked while
        // starting up.  Kill!
        control.shutdown()
      } else {
        val future = partitionsInStartup(tp)
        subSources += (tp -> (control -> future))
        partitionsInStartup -= tp
      }
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit =
      emitSubSourcesForPendingPartitions()
    override def onDownstreamFinish(): Unit =
      performShutdown()
  })

  private def updatePendingPartitionsAndEmitSubSources(formerlyUnknownPartitions: Set[TopicPartition]): Unit = {
    pendingPartitions ++= formerlyUnknownPartitions.filter(!partitionsInStartup.contains(_))
    emitSubSourcesForPendingPartitions()
  }

  @tailrec
  private def emitSubSourcesForPendingPartitions(): Unit =
    if (pendingPartitions.nonEmpty && isAvailable(shape.out)) {
      val tp = pendingPartitions.head

      pendingPartitions = pendingPartitions.tail
      val promise = Promise[Done]()
      partitionsInStartup += tp -> promise.future
      val subSource = Source.fromGraph(
        new SubSourceStage(tp,
                           consumerActor,
                           subsourceStartedCB,
                           subsourceCancelledCB,
                           messageBuilder = this,
                           actorNumber)
      )
      push(shape.out, (tp, promise, subSource))
      emitSubSourcesForPendingPartitions()
    }

  override def postStop(): Unit = {
    consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
    onShutdown()
    super.postStop()
  }

  override def performStop(): Unit = {
    setKeepGoing(true)
    subSources.foreach {
      case (_, (control, _)) => control.stop()
    }
    complete(shape.out)
    onStop()
  }

  override def performShutdown(): Unit = {
    setKeepGoing(true)
    //todo we should wait for subsources to be shutdown and next shutdown main stage
    subSources.foreach {
      case (_, (control, _)) => control.shutdown()
    }

    if (!isClosed(shape.out)) {
      complete(shape.out)
    }
    sourceActor.become {
      case (_, Terminated(ref)) if ref == consumerActor =>
        onShutdown()
        completeStage()
    }
    materializer.scheduleOnce(settings.stopTimeout, new Runnable {
      override def run(): Unit =
        consumerActor.tell(KafkaConsumerActor.Internal.Stop, sourceActor.ref)
    })
  }

}
