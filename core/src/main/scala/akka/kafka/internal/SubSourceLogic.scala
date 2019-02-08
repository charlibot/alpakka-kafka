/*
 * Copyright (C) 2014 - 2016 Softwaremill <http://softwaremill.com>
 * Copyright (C) 2016 - 2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.kafka.internal

import akka.Done
import akka.actor.Status
import akka.actor.{ActorRef, ExtendedActorSystem, Terminated}
import akka.annotation.InternalApi
import akka.kafka.Subscriptions.{TopicSubscription, TopicSubscriptionPattern}
import akka.kafka._
import akka.kafka.scaladsl.Consumer.Control
import akka.pattern.{ask, AskTimeoutException}
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStageLogic.StageActor
import akka.stream.stage.{StageLogging, _}
import akka.stream.{ActorMaterializerHelper, Attributes, Outlet, SourceShape}
import akka.util.Timeout
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * Internal API.
 *
 * Anonymous sub-class instance is created in [[CommittableSubSource]].
 */
@InternalApi
private abstract class SubSourceLogic[K, V, Msg](
    val shape: SourceShape[(TopicPartition, Source[Msg, Control])],
    settings: ConsumerSettings[K, V],
    subscription: AutoSubscription,
    getOffsetsOnAssign: Option[Set[TopicPartition] => Future[Map[TopicPartition, Long]]] = None,
    onRevoke: Set[TopicPartition] => Unit = _ => ()
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
  var partitionsInStartup: immutable.Set[TopicPartition] = immutable.Set.empty
  var subSources: Map[TopicPartition, Control] = immutable.Map.empty

  /** Kafka has signalled these partitions are revoked, but some may be re-assigned just after revoking. */
  var partitionsToRevoke: Set[TopicPartition] = Set.empty

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
            listenerRef =>
              // TODO: Make a new timeout
              val listenerCompletion =
                listenerRef.ask(TopicPartitionsAssigned(subscription, assignedTps))(settings.waitClosePartition,
                                                                                    sourceActor.ref)
              val completedListener = Try(Await.result(listenerCompletion, settings.waitClosePartition))
              completedListener match {
                case Success(_) =>
                case Failure(_) =>
                  log.warning(
                    s"Listener did not respond to TopicPartitionsAssigned within ${settings.waitClosePartition}"
                  )
              }
          }
          partitionAssignedCB(assignedTps)
        },
        revokedTps => {
          subscription.rebalanceListener.foreach {
            listenerRef =>
              // TODO: Make a new timeout
              val listenerCompletion =
                listenerRef.ask(TopicPartitionsRevoked(subscription, revokedTps))(settings.waitClosePartition,
                                                                                  sourceActor.ref)
              val completedListener = Try(Await.result(listenerCompletion, settings.waitClosePartition))
              completedListener match {
                case Success(_) =>
                case Failure(_) =>
                  log.warning(
                    s"Listener did not respond to TopicPartitionsRevoked within ${settings.waitClosePartition}"
                  )
              }
          }
          partitionRevokedCB(revokedTps)
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

  def partitionAssignedCB(assigned: Set[TopicPartition]): Unit = {
    val formerlyKnown = assigned intersect partitionsToRevoke
    val requriesRestarting = formerlyKnown.filter(tp => subSources.get(tp).exists(_.hasShutdown))
    val formerlyUnknown = assigned -- partitionsToRevoke
    val toStart = formerlyUnknown ++ requriesRestarting

    if (log.isDebugEnabled && toStart.nonEmpty) {
      log.debug("#{} Assigning new partitions: {}", actorNumber, toStart.mkString(", "))
    }

    // make sure re-assigned partitions don't get closed on CloseRevokedPartitions timer
    partitionsToRevoke = partitionsToRevoke -- assigned

    implicit val ec: ExecutionContext = materializer.executionContext

    val emittedSubSources = getOffsetsOnAssign match {
      case None =>
//        updatePendingPartitionsAndEmitSubSources(formerlyUnknown)
        updatePendingPartitionsAndEmitSubSourcesCb.invokeWithFeedback(toStart)

      case Some(getOffsetsFromExternal) =>
        getOffsetsFromExternal(assigned)
          .flatMap(seekAndEmitSubSources(toStart, _))
          .recoverWith {
            case exception: Exception =>
              stageFailCB.invokeWithFeedback(
                new ConsumerFailed(
                  s"#$actorNumber Failed to fetch offset for partitions: ${toStart.mkString(", ")}.",
                  exception
                )
              )
          }
    }

    emittedSubSources.map { _ =>
      if (log.isDebugEnabled) {
        log.debug("#{} Closing SubSources for revoked partitions: {}", actorNumber, partitionsToRevoke.mkString(", "))
      }
      onRevoke(partitionsToRevoke)
      pendingPartitions --= partitionsToRevoke
      partitionsInStartup --= partitionsToRevoke
      partitionsToRevoke.flatMap(subSources.get).foreach(_.shutdown())
      subSources --= partitionsToRevoke
      partitionsToRevoke = Set.empty
      log.info("Finished ...")
    }
  }

  private def seekAndEmitSubSources(
      toStartPartitions: Set[TopicPartition],
      offsets: Map[TopicPartition, Long]
  ): Future[Done] = {
    implicit val ec: ExecutionContext = materializer.executionContext
    log.info("Seeking and emitting sub sources")
    consumerActor
      .ask(KafkaConsumerActor.Internal.Seek(offsets))(Timeout(10.seconds), sourceActor.ref)
      .flatMap(_ => updatePendingPartitionsAndEmitSubSourcesCb.invokeWithFeedback(toStartPartitions))
      .recoverWith {
        case _: AskTimeoutException =>
          stageFailCB.invokeWithFeedback(
            new ConsumerFailed(
              s"#$actorNumber Consumer failed during seek for partitions: ${offsets.keys.mkString(", ")}."
            )
          )
      }
  }

  def partitionRevokedCB(revoked: Set[TopicPartition]): Unit =
    partitionsToRevoke ++= revoked

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
            seekAndEmitSubSources(toStartPartitions = Set.empty, Map(tp -> record.offset()))
          case None => emitSubSourcesForPendingPartitions()
        }
    }

  val subsourceStartedCB: AsyncCallback[(TopicPartition, Control)] = getAsyncCallback[(TopicPartition, Control)] {
    case (tp, control) =>
      if (!partitionsInStartup.contains(tp)) {
        // Partition was revoked while
        // starting  up. Kill!
        control.shutdown()
      } else {
        subSources += (tp -> control)
        partitionsInStartup -= tp
      }
  }

  setHandler(shape.out, new OutHandler {
    override def onPull(): Unit =
      emitSubSourcesForPendingPartitions()
    override def onDownstreamFinish(): Unit =
      performShutdown()
  })

  private def updatePendingPartitionsAndEmitSubSources(toStartPartitions: Set[TopicPartition]): Unit = {
    pendingPartitions ++= toStartPartitions.filter(!partitionsInStartup.contains(_))
    log.info("About to emit sub sources")
    emitSubSourcesForPendingPartitions()
  }

  @tailrec
  private def emitSubSourcesForPendingPartitions(): Unit =
    if (pendingPartitions.nonEmpty && isAvailable(shape.out)) {
      val tp = pendingPartitions.head
      log.info(s"Emitting $tp")

      pendingPartitions = pendingPartitions.tail
      partitionsInStartup += tp
      val subSource = Source.fromGraph(
        new SubSourceStage(tp,
                           consumerActor,
                           subsourceStartedCB,
                           subsourceCancelledCB,
                           messageBuilder = this,
                           actorNumber)
      )
      push(shape.out, (tp, subSource))
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
      case (_, control) => control.stop()
    }
    complete(shape.out)
    onStop()
  }

  override def performShutdown(): Unit = {
    setKeepGoing(true)
    //todo we should wait for subsources to be shutdown and next shutdown main stage
    subSources.foreach {
      case (_, control) => control.shutdown()
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

/** Internal API */
private final class SubSourceStage[K, V, Msg](
    tp: TopicPartition,
    consumerActor: ActorRef,
    subSourceStartedCb: AsyncCallback[(TopicPartition, Control)],
    subSourceCancelledCb: AsyncCallback[(TopicPartition, Option[ConsumerRecord[K, V]])],
    messageBuilder: MessageBuilder[K, V, Msg],
    actorNumber: Int
) extends GraphStageWithMaterializedValue[SourceShape[Msg], Control] { stage =>
  val out = Outlet[Msg]("out")
  val shape = new SourceShape(out)

  private def logic(): GraphStageLogic with Control =
    new GraphStageLogic(shape) with PromiseControl with MetricsControl with StageLogging {
      override def executionContext: ExecutionContext = materializer.executionContext
      override def consumerFuture: Future[ActorRef] = Future.successful(consumerActor)
      val shape = stage.shape
      val requestMessages = KafkaConsumerActor.Internal.RequestMessages(0, Set(tp))
      var requested = false
      var subSourceActor: StageActor = _
      var buffer: Iterator[ConsumerRecord[K, V]] = Iterator.empty

      override def preStart(): Unit = {
        log.debug("#{} Starting SubSource for partition {}", actorNumber, tp)
        super.preStart()
        subSourceStartedCb.invoke(tp -> this.asInstanceOf[Control])
        subSourceActor = getStageActor {
          case (_, msg: KafkaConsumerActor.Internal.Messages[K, V]) =>
            requested = false
            // do not use simple ++ because of https://issues.scala-lang.org/browse/SI-9766
            if (buffer.hasNext) {
              buffer = buffer ++ msg.messages
            } else {
              buffer = msg.messages
            }
            pump()
          case (_, Status.Failure(e)) =>
            failStage(e)
          case (_, Terminated(ref)) if ref == consumerActor =>
            failStage(new ConsumerFailed)
        }
        subSourceActor.watch(consumerActor)
      }

      override def postStop(): Unit = {
        onShutdown()
        super.postStop()
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit =
            pump()

          override def onDownstreamFinish(): Unit = {
            val firstUnconsumed = if (buffer.hasNext) {
              Some(buffer.next())
            } else {
              None
            }

            subSourceCancelledCb.invoke(tp -> firstUnconsumed)
            super.onDownstreamFinish()
          }
        }
      )

      def performShutdown() = {
        log.debug("#{} Completing SubSource for partition {}", actorNumber, tp)
        completeStage()
      }

      @tailrec
      private def pump(): Unit =
        if (isAvailable(out)) {
          if (buffer.hasNext) {
            val msg = buffer.next()
            push(out, messageBuilder.createMessage(msg))
            pump()
          } else if (!requested) {
            requested = true
            consumerActor.tell(requestMessages, subSourceActor.ref)
          }
        }
    }

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Control) = {
    val result = logic()
    (result, result)
  }
}
