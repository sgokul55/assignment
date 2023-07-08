package frontend

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import backend.SetCollector
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.duration.DurationInt

class RGBEventManagerTest extends AnyWordSpec
  with BeforeAndAfterAll
  with Matchers {
  val config = ConfigFactory.parseString(
    """
        conviva.max-records = 10
        conviva.max-nr-of-aged-collectors = 2
  """
  )

  val testKit = ActorTestKit(config)

  "Given the Raw Event to the manager" must {
    val epoch = Instant.now().toEpochMilli
    "for multiple same kind of raw events, forward batch to SetCollector" in {
      val probe = testKit.createTestProbe[SetCollector.Command]()
      val initState = RGBEventManager.State(Some(probe.ref), Vector.empty, 1)
      val manager = testKit.spawn(RGBEventManager(Some(initState)), "manager-1")
      (1 to 100).foreach { i =>
        manager ! RGBEventManager.InputEvent(s"R_${epoch + i}")
      }
      val batch = probe.receiveMessage(10.seconds).asInstanceOf[SetCollector.BatchedCommand]
      assert(batch.seq.size == 100)
    }
    "for multiple different kind of raw events, forward grouped by event type batch to SetCollector" in {
      val probe = testKit.createTestProbe[SetCollector.Command]()
      val initState = RGBEventManager.State(Some(probe.ref), Vector.empty, 1)
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
    }

  }

  "Given Retirement Command" must {
    "Activate new collector and maintain old rgb set collectors" in {
      val probe = testKit.createTestProbe[SetCollector.Command]()
      val resultProbe = testKit.createTestProbe[RGBEventManager.State]()
      val initState = RGBEventManager.State(Some(probe.ref), Vector.empty, 1)
      val manager = testKit.spawn(RGBEventManager(Some(initState)), "manager-21")
      // send retirement request
      manager ! RGBEventManager.RetirementRequest(Instant.now(), Instant.now(), probe.ref)
      manager ! RGBEventManager.GetManagerState(resultProbe.ref)
      val state = resultProbe.receiveMessage()
      assert(state.agedCollectors.size == 1)
    }

    "Remove oldest rgb set collectors from aged actor list" in {
      val probe = testKit.createTestProbe[SetCollector.Command]()
      val resultProbe = testKit.createTestProbe[RGBEventManager.State]()
      //      val initState = RGBEventManager.State(Some(probe.ref), Vector.empty, 1)
      val manager = testKit.spawn(RGBEventManager(), "manager-3")
      manager ! RGBEventManager.GetManagerState(resultProbe.ref)
      val s = resultProbe.receiveMessage()
      // send retirement request
      manager ! RGBEventManager.RetirementRequest(Instant.now(), Instant.now(), s.activeCollector.get)
      manager ! RGBEventManager.RetirementRequest(Instant.now(), Instant.now(), probe.ref)
      manager ! RGBEventManager.GetManagerState(resultProbe.ref)
      val state = resultProbe.receiveMessage()
      manager ! RGBEventManager.RetirementRequest(Instant.now(), Instant.now(), probe.ref)
      manager ! RGBEventManager.GetManagerState(resultProbe.ref)
      val modifiedState = resultProbe.receiveMessage()
      assert(!modifiedState.agedCollectors.contains(s.activeCollector.get))
    }

  }

}
