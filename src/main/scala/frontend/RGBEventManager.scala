package frontend

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import backend.SetCollector
import backend.SetCollector.RGB_Set
import org.slf4j.Logger

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
 *
 * It forwards the Stream data by micro-batching it to current active Set Collector.
 * When current collector aged, append it to vector with first events start time and last event's end time.
 *
 * using agedCollector, for given start and end time, we can collect list of actors to query data from!!
 * So query is issued to multiple actors, reach responds its data. we just need to merge and send to the requester.
 *
 * agedCollectors -> (startTime, endTime, actorRef)
 */
object RGBEventManager {

  val QueueSize = 10000

  def apply(state: Option[State] = None): Behavior[ManagerCommand] = Behaviors.setup { context =>
    implicit val log: Logger = context.log
    // for helping unit test
    val initialState = if (state.isEmpty) {
      val maxCollectors = context.system.settings.config.getInt("conviva.max-nr-of-aged-collectors")
      val initialActor = context.spawn(SetCollector.apply(), s"set_collector_${Instant.now().toEpochMilli}")
      State(Some(initialActor), Vector.empty, maxCollectors)
    } else
      state.get
    val q = queue(context.self)(context.system, context.executionContext)
    activate(initialState, q)(context)
  }

  private def activate(state: State, queue: SourceQueueWithComplete[String])(implicit context: ActorContext[ManagerCommand]): Behaviors.Receive[ManagerCommand] = {
    Behaviors.receiveMessage {
      case i: InputEvent =>
        context.log.debug("Enqueuing Event {}", i.string)
        queue.offer(i.string)
        Behaviors.same
      case m: MicroBatchedEvents =>
        state.activeCollector.foreach(actor => actor ! SetCollector.BatchedCommand(m.list, context.self))
        Behaviors.same
      case r: RetirementRequest =>
        val newActor = context.spawn(SetCollector(), s"set_collector_${Instant.now().toEpochMilli}_${Random.nextInt()}")
        val s = state.copy(
          activeCollector = Some(newActor),
          agedCollectors = state.agedCollectors :+ (r.startTime, r.endTime, r.replyTo)
        )
        val finalState = if (state.maxNoOfAgedCollector < s.agedCollectors.size) {
          // stop the age old actor!
          context.stop(state.agedCollectors(0)._3)
          s.copy(
            agedCollectors = s.agedCollectors.takeRight(1)
          )
        } else {
          s
        }
        activate(finalState, queue)
      case r: ReturnUnprocessed =>
        state.activeCollector.foreach(actor => actor ! SetCollector.BatchedCommand(r.list, context.self))
        Behaviors.same
      case g: GetManagerState =>
        g.replyTo ! state
        Behaviors.same
      case q: Query =>
        // sent the query matching old and current active actors.
        val filtered = state.agedCollectors.filter(a => Instant.ofEpochMilli(q.start).isBefore(a._2))
        filtered.foreach(a => a._3 ! SetCollector.Query(q.start, q.end, q.replyTo))
        state.activeCollector.foreach(a => a ! SetCollector.Query(q.start, q.end, q.replyTo))
        Behaviors.same
    }
  }

  private def queue(actorRef: ActorRef[ManagerCommand])(implicit system: ActorSystem[_], ec: ExecutionContext): SourceQueueWithComplete[String] = {
    Source.queue[String](QueueSize, OverflowStrategy.dropNew)
      .async
      .groupBy(3, f => f.substring(0, 1)) // order by R, G, B
      .map { s =>
        val epochStr: String = s.split("_")(1)
        val epoch = epochStr.toLong
        if (s.startsWith("R_")) {
          SetCollector.Red("red", epoch)
        } else if (s.startsWith("G_")) {
          SetCollector.Green("green", epoch)
        } else {
          SetCollector.Blue("blue", epoch)
        }
      }
      .groupedWithin(100, 20.milli)
      .async
      .mergeSubstreams
      .async
      .mapAsync(5) { requests: Seq[SetCollector.Command] =>
        Future {
          actorRef ! MicroBatchedEvents(requests)
        }

      }
      .toMat(Sink.ignore)(Keep.left)
      .run()
  }

  trait ManagerCommand

  case class InputEvent(string: String) extends ManagerCommand

  case class MicroBatchedEvents(list: Seq[SetCollector.Command]) extends ManagerCommand

  case class RetirementRequest(startTime: Instant, endTime: Instant, replyTo: ActorRef[SetCollector.Command]) extends ManagerCommand

  case class ReturnUnprocessed(list: Seq[SetCollector.Command]) extends ManagerCommand

  case class GetManagerState(replyTo: ActorRef[State]) extends ManagerCommand

  final case class Query(start: Long, end: Long, replyTo: ActorRef[List[RGB_Set]]) extends ManagerCommand

  case class State(
                    activeCollector: Option[ActorRef[SetCollector.Command]],
                    agedCollectors: Vector[(Instant, Instant, ActorRef[SetCollector.Command])],
                    maxNoOfAgedCollector: Int
                  )


}
