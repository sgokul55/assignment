package backend.actor

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.util.Random

class SetCollectorTest extends AnyWordSpec
  with BeforeAndAfterAll
  with Matchers {

  val testKit = ActorTestKit()

  "Given R, G, B messages" must {
    val actor = testKit.spawn(SetCollector(), "rgbCollector-1")
    val epoch = Instant.now().toEpochMilli

    "For single R, place it internal state" in {
      //val actor = testKit.spawn(SetCollector(), "rgbCollector-1")
      val probe = testKit.createTestProbe[SetCollector.State]()
      import backend.actor.SetCollector._
      val expectedState = State(Vector(RGB_Set(Red("red", epoch), None, None)), Vector.empty, 0, 0)
      actor ! SetCollector.Red("red", epoch)
      actor ! SetCollector.GetState(probe.ref)
      probe.expectMessage(expectedState)
    }
    "For valid R, G, B, Form a valid set" in {
      //val actor = testKit.spawn(SetCollector(), "rgbCollector-2")
      val probe = testKit.createTestProbe[SetCollector.State]()
      import backend.actor.SetCollector._
      val expectedState = State(
        Vector(RGB_Set(Red("red", epoch), Some(Green("green", epoch + 1)), Some(Blue("blue", epoch + 2)))),
        Vector.empty,
        1,
        1
      )
      actor ! SetCollector.Green("green", epoch + 1)
      actor ! SetCollector.Blue("blue", epoch + 2)
      actor ! SetCollector.GetState(probe.ref)
      probe.expectMessage(expectedState)
    }
  }

  "Given Burst of R, G, B messages of 10K" must {
    val actor = testKit.spawn(SetCollector(), "rgbCollector-2")
    val epoch = Instant.now().toEpochMilli

    "For 10K R, place it internal state" in {
      val probe = testKit.createTestProbe[SetCollector.State]()
      (1 to 10_000).foreach { i =>
        actor ! SetCollector.Red(s"red-$i", epoch + i)
      }
      actor ! SetCollector.GetState(probe.ref)
      val state: SetCollector.State = probe.receiveMessage(3.seconds)
      val sets: Seq[SetCollector.RGB_Set] = state.list
      assert(sets.size == 10_000)
      sets.foreach(s => assert(s.g.isEmpty && s.b.isEmpty))
    }

    "For 10K G, place it internal state" in {
      val probe = testKit.createTestProbe[SetCollector.State]()
      (1 to 10_000).foreach { i =>
        actor ! SetCollector.Green(s"Green-$i", epoch + i + 1)
      }
      actor ! SetCollector.GetState(probe.ref)
      val state: SetCollector.State = probe.receiveMessage(3.seconds)
      val sets: Seq[SetCollector.RGB_Set] = state.list
      assert(sets.size == 10_000)
      sets.foreach(s => assert(s.g.nonEmpty && s.b.isEmpty))
    }
    "For 10K B, place it internal state" in {
      val probe = testKit.createTestProbe[SetCollector.State]()
      (1 to 10_000).foreach { i =>
        actor ! SetCollector.Blue(s"Blue-$i", epoch + i + 2)
      }
      actor ! SetCollector.GetState(probe.ref)
      val state: SetCollector.State = probe.receiveMessage(3.seconds)
      val sets: Seq[SetCollector.RGB_Set] = state.list
      assert(sets.size == 10_000)
      sets.foreach(s => assert(s.g.nonEmpty && s.b.nonEmpty))
      val rand = Random
      val indexRand = rand.nextInt(10_000)
      val rgb = sets(indexRand)
      println(s"Randomly checking the index ${indexRand}")
      assert(rgb.r.timestamp == rgb.g.get.timestamp - 1)
      assert(rgb.r.timestamp == rgb.b.get.timestamp - 2)
      assert(state.blueHead == 10_000)
      assert(state.greenHead == 10_000)
    }
  }

  "Given out of order of R, G, B" must {
    val actor = testKit.spawn(SetCollector(), "rgbCollector-2")
    val epoch = Instant.now().toEpochMilli

    "For 10K R, place it internal state" in {

    }

  }


  override def afterAll(): Unit = testKit.shutdownTestKit()

}
