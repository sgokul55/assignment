package backend.actor

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import frontend.RGBEventManager
import org.slf4j.Logger

import java.time.Instant

object SetCollector {

  // for query - + or - 50 millisecond accuracy
  val accuracyLevel = 50

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    implicit val log: Logger = context.log
    val maxRecords: Int = context.system.settings.config.getConfig("conviva").getInt("max-records")
    val initialState = State(Vector.empty[RGB_Set], Vector.empty, 0, 0, 0, maxRecords)
    collectRGBSets(initialState)(log, context)
  }

  private def collectRGBSets(state: State)(implicit log: Logger, context: ActorContext[Command]): Behaviors.Receive[Command] = {
    Behaviors.receiveMessage {
      case r: Red =>
        log.debug("Red message arrived {}", r)
        val updatedState = handleRed(r, state)
        collectRGBSets(updatedState)
      case g: Green =>
        log.debug("Green message arrived {}", g)
        val updatedState = handleGreen(g, state)
        collectRGBSets(updatedState)
      case b: Blue =>
        log.debug("Blue message arrived {}", b)
        val updatedState = handleBlue(b, state)
        collectRGBSets(updatedState)
      case lst: BatchedCommand =>
        val batch: Seq[Command] = lst.seq
        val updatedState = handleBatchRequest(batch, state)
        val outOfOrderHandledState = handleOutOfOrderMessages(updatedState)
        if (outOfOrderHandledState.validSets >= outOfOrderHandledState.maxRecords) {
          // there can be some more in-flight messages.
          log.warn("I am retiring from set collection. Total processed sets {} ", state.validSets)
          val startTime = outOfOrderHandledState.list.head.r.timestamp
          val endTime = outOfOrderHandledState.list.last.r.timestamp
          lst.replyTo ! RGBEventManager
            .RetirementRequest(
              Instant.ofEpochMilli(startTime),
              Instant.ofEpochMilli(endTime),
              context.self
            )
          val finalState = outOfOrderHandledState.copy(outOfOrderMessages = Vector.empty)
          queryOnlyMode(finalState)
        } else {
          collectRGBSets(outOfOrderHandledState)
        }
      case s: GetState =>
        s.replyTo ! state
        Behaviors.same
      case q: Query =>
        // given start and end time, return all the RGB_Sets
        // sIndex cant be -1 as manager decided this
        val sIndex = binraySearch(state.list, q.start, 0, state.list.size)
        // it can be -1, the remaining, can be spill over in next actor
        val eIndex = binraySearch(state.list, q.end, 0, state.list.size)
        if (sIndex != -1) {
          val qResult: Seq[RGB_Set] = if (eIndex == -1) {
            state.list.takeRight(state.list.size - sIndex)
          } else {
            state.list.take(state.list.size - eIndex)
          }
          q.replyTo ! qResult.toList
        } else {
          q.replyTo ! List.empty[RGB_Set]
        }
        Behaviors.same
    }
  }

  private def binraySearch(list: Seq[RGB_Set], key: Long, l: Int, r: Int): Int = {
    if (l >= r) -1
    else {
      val mid = l + (r - l) / 2
      if (list(mid).r.timestamp == key || Math.abs(list(mid).r.timestamp - key) <= accuracyLevel)
        mid
      else if (list(mid).r.timestamp < key) {
        binraySearch(list, key, mid + 1, r)
      } else {
        binraySearch(list, key, l, mid)
      }
    }
  }

  private def queryOnlyMode(state: State): Behaviors.Receive[Command] = {
    Behaviors.receiveMessage {
      case s: GetState =>
        s.replyTo ! state
        Behaviors.same
      case lst: BatchedCommand =>
        lst.replyTo ! RGBEventManager.ReturnUnprocessed(lst.seq)
        Behaviors.same
    }
  }

  private def handleBatchRequest(a: Seq[Command], state: State)(implicit logger: Logger): State = {
    logger.debug("Handling batch requests of size {} and head message - {}", a.size, a.head)
    var s = state
    a.foreach {
      case red: Red =>
        s = handleRed(red, s)
      case green: Green =>
        s = handleGreen(green, s)
      case blue: Blue =>
        s = handleBlue(blue, s)
    }
    s
  }

  private def handleRed(red: Red, state: State)(implicit log: Logger): State = {
    val s = state.copy(
      list = state.list :+ RGB_Set(red, None, None)
    )
    if (s.outOfOrderMessages.nonEmpty) {
      handleOutOfOrderMessages(s)
    } else {
      s
    }
  }

  private def handleGreen(green: Green, state: State, accumulateOOOM: Boolean = true)(implicit log: Logger): State = {
    // the red is already in sorted order!
    if (state.list.isEmpty || state.list.size < state.greenHead + 1) {
      log.debug("the current green is out of order")
      if (accumulateOOOM) state.copy(outOfOrderMessages = state.outOfOrderMessages :+ green)
      else state
    } else {
      val gIndex = state.greenHead
      val set = state.list(gIndex)
      if (set.r.timestamp <= green.timestamp) {
        log.debug("Forming set with given green at {}", gIndex)
        val updatedList = state.list.updated(gIndex, set.copy(g = Some(green)))
        if (accumulateOOOM)
          state.copy(list = updatedList, greenHead = gIndex + 1)
        else {
          val updated = removeFirst[Command](state.outOfOrderMessages, green)
          state.copy(list = updatedList, greenHead = gIndex + 1, outOfOrderMessages = updated)
        }
      } else {
        log.warn("Dropping green as no place. {}", green)
        val updated = removeFirst[Command](state.outOfOrderMessages, green)
        state.copy(outOfOrderMessages = updated)
      }
    }
  }

  private def handleBlue(blue: Blue, state: State, accumulateOOOM: Boolean = true)(implicit log: Logger): State = {
    if (state.list.isEmpty || state.greenHead < state.blueHead + 1) {
      log.debug("the current blue is out of order")
      if (accumulateOOOM) state.copy(outOfOrderMessages = state.outOfOrderMessages :+ blue)
      else state
    } else {
      val bIndex = state.blueHead
      val set = state.list(bIndex)
      if (set.g.get.timestamp <= blue.timestamp) {
        log.debug("Forming set with given blue at {}", bIndex)
        val updatedList = state.list.updated(bIndex, set.copy(b = Some(blue)))
        if (accumulateOOOM) state.copy(list = updatedList, blueHead = bIndex + 1, validSets = state.validSets + 1)
        else {
          val updated = removeFirst[Command](state.outOfOrderMessages, blue)
          state.copy(list = updatedList, blueHead = bIndex + 1, validSets = state.validSets + 1, outOfOrderMessages = updated)
        }
      } else {
        log.warn("Dropping blue as no place. {}", blue)
        val updated = removeFirst[Command](state.outOfOrderMessages, blue)
        state.copy(outOfOrderMessages = updated)
      }
    }
  }

  /**
   * try to place the messages in right spot for given time.
   * if time elapses -> drop all out of order msgs.
   * R -> never be part of ooo msg, as it is first elm in the set.
   * G -> wait arrival of R
   * B -> wait for G to find its spot
   *
   */
  private def handleOutOfOrderMessages(state: State)(implicit logger: Logger): State = {
    if (state.outOfOrderMessages.nonEmpty) {
      val ooom = state.outOfOrderMessages
      var s = state
      ooom.foreach {
        case g: Green =>
          logger.debug("Handling out of order - Green msg - {}", g)
          s = handleGreen(g, s, false)
        case b: Blue =>
          logger.debug("Handling out of order - Blue msg - {}", b)
          s = handleBlue(b, s, false)
      }
      s
    } else
      state
  }

  private def removeFirst[T](xs: Vector[T], x: T) = {
    val i = xs.indexOf(x)
    if (i == -1) xs else xs.patch(i, Nil, 1)
  }

  sealed trait Command

  final case class Red(name: String, timestamp: Long) extends Command

  final case class Green(name: String, timestamp: Long) extends Command

  final case class Blue(name: String, timestamp: Long) extends Command

  final case class BatchedCommand(seq: Seq[Command], replyTo: ActorRef[RGBEventManager.ManagerCommand]) extends Command

  final case class GetState(replyTo: ActorRef[State]) extends Command

  final case class Query(start: Long, end: Long, replyTo: ActorRef[List[RGB_Set]]) extends Command

  case class RGB_Set(r: Red, g: Option[Green], b: Option[Blue])

  case class State(
                    list: Vector[RGB_Set], // all valid sets
                    outOfOrderMessages: Vector[Command], // non matchable -> G, B
                    greenHead: Int = 0,
                    blueHead: Int = 0,
                    validSets: Int = 0,
                    maxRecords: Int
                  )

}
