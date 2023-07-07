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
      case s: GetState =>
        s.replyTo ! state
        Behaviors.same
    }
  }

  private def handleRed(red: Red, state: State)(implicit log: Logger): State = {
    val s = state.copy(
      list = state.list :+ RGB_Set(red, None, None)
    )
    if (state.outOfOrderMessages.nonEmpty) {
      // fill the out of order msgs
    }
    s
  }

  private def handleGreen(green: Green, state: State)(implicit log: Logger): State = {
    // the red is already in sorted order!
    val gIndex = state.greenHead
    val set = state.list(gIndex)
    if (set.r.timestamp <= green.timestamp) {
      println(s"Forming set with given green at ${gIndex}")
      val updatedList = state.list.updated(gIndex, set.copy(g = Some(green)))
      state.copy(list = updatedList, greenHead = gIndex + 1)
    } else {
      println(s"Dropping green as no place")
      state
    }
  }

  private def handleBlue(blue: Blue, state: State)(implicit log: Logger): State = {
    val bIndex = state.blueHead
    val set = state.list(bIndex)
    if (set.g.isEmpty) {
      // the current blue is out of order!!
      state.copy(outOfOrderMessages = state.outOfOrderMessages :+ blue)
    } else if (set.g.exists(g => g.timestamp <= blue.timestamp)) {
      println(s"Forming set with given Blue at ${bIndex}")
      val updatedList = state.list.updated(bIndex, set.copy(b = Some(blue)))
      state.copy(list = updatedList, blueHead = bIndex + 1)
    } else {
      println(s"Dropping blue as no place")
      state
    }
  }

  private def handleOutOfOrderMessages(vector: Vector[Command]) = {
    val a: Seq[Command] = vector.filter { g =>
      g match {
        case g: Green =>
          true
        case _ => false
      }
    }
  }

  sealed trait Command

  final case class Red(name: String, timestamp: Long) extends Command

  final case class Green(name: String, timestamp: Long) extends Command

  final case class Blue(name: String, timestamp: Long) extends Command

  final case class GetState(replyTo: ActorRef[State]) extends Command

  case class RGB_Set(r: Red, g: Option[Green], b: Option[Blue]) {
    def addGreen(green: Green): RGB_Set = {
      if (canAddGreen(green)) copy(g = Some(green))
      else this
    }

    def canAddGreen(green: Green): Boolean = {
      g.isEmpty && r.timestamp < green.timestamp
    }

  }

  case class State(
                    list: Vector[RGB_Set], // all valid sets
                    outOfOrderMessages: Vector[Command], // non matchable -> G, B
                    greenHead: Int = 0,
                    blueHead: Int = 0
                  )

  // front end
  // s1, s2, s3 ---> 3 sources are merge sorted by time stamp
  // get the total pairs
  // time query if possible

  // backend - one active backend at a time
  // receives batch of sorted events by manager
  // fills red
  // fills green and blue based on condition
  // binary search


}
