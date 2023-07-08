package frontend

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import backend.actor.SetCollector
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.duration.DurationInt

class RGBEventManagerTest extends AnyWordSpec
  with BeforeAndAfterAll
  with Matchers {

  val testKit = ActorTestKit()

  "Given the Raw Event to the manager" must {
    val epoch = Instant.now().toEpochMilli
    "for multiple same kind of raw events, forward batch to SetCollector" in {
      val probe = testKit.createTestProbe[SetCollector.Command]()
      val initState = RGBEventManager.State(Some(probe.ref), Vector.empty)
      val manager = testKit.spawn(RGBEventManager(Some(initState)), "manager-1")
      (1 to 100).foreach { i =>
        manager ! RGBEventManager.InputEvent(s"R_${epoch + i}")
      }
      val batch = probe.receiveMessage(10.seconds).asInstanceOf[SetCollector.BatchedCommand]
      assert(batch.seq.size == 100)
    }
    "for multiple different kind of raw events, forward grouped by event type batch to SetCollector" in {
      val probe = testKit.createTestProbe[SetCollector.Command]()
      val initState = RGBEventManager.State(Some(probe.ref), Vector.empty)
      val manager = testKit.spawn(RGBEventManager(Some(initState)), "manager-2")
      (1 to 100).foreach { i =>
        if (i % 3 == 0) {
          manager ! RGBEventManager.InputEvent(s"G_${epoch + i}")
        } else if (i % 3 == 1) {
          manager ! RGBEventManager.InputEvent(s"R_${epoch + i}")
        } else {
          manager ! RGBEventManager.InputEvent(s"B_${epoch + i}")
        }

      }
      val batch = probe.receiveMessages(3).asInstanceOf[Seq[SetCollector.BatchedCommand]]
      assert(batch.size == 3)
      assert(batch(0).seq.size + batch(1).seq.size + batch(2).seq.size == 100)
    }

  }

  "Given Retirement Command" must {
    "Activate new collector and maintain old rgb set collectors" in {

    }

  }

}
