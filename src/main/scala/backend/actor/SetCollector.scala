package backend.actor

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import org.slf4j.Logger

object SetCollector {

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    implicit val log: Logger = context.log
    val initialState = State(Vector.empty[RGB_Set], Vector.empty, 0, 0)
    collectRGBSets(initialState)
  }

  private def collectRGBSets(state: State)(implicit log: Logger): Behaviors.Receive[Command] = {
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
        val a: Seq[Command] = lst.seq
        val updatedState = handleBatchRequest(a, state)
        val outOfOrderHandledState = handleOutOfOrderMessages(updatedState)
        collectRGBSets(outOfOrderHandledState)
      case s: GetState =>
        s.replyTo ! state
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

  /**
   * try to place the messages in right spot for given time.
   * if time elapses -> drop all out of order msgs.
   * R -> never be part of ooo msg, as it is first elm in the set.
   * G -> wait arrival of R
   * B -> wait for G to find its spot
   *
   * @param state
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
        state
      }
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
        state
      }
    }
  }

  private def removeFirst[T](xs: Vector[T], x: T) = {
    val i = xs.indexOf(x)
    if (i == -1) xs else xs.patch(i, Nil, 1)
  }

  sealed trait Command

  final case class Red(name: String, timestamp: Long) extends Command

  final case class Green(name: String, timestamp: Long) extends Command

  final case class Blue(name: String, timestamp: Long) extends Command

  final case class BatchedCommand(seq: Seq[Command]) extends Command

  final case class GetState(replyTo: ActorRef[State]) extends Command

  case class RGB_Set(r: Red, g: Option[Green], b: Option[Blue])

  case class State(
                    list: Vector[RGB_Set], // all valid sets
                    outOfOrderMessages: Vector[Command], // non matchable -> G, B
                    greenHead: Int = 0,
                    blueHead: Int = 0,
                    validSets: Int = 0
                  )

}
