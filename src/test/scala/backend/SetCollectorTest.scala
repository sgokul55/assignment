package backend

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import backend.SetCollector._
import com.typesafe.config.ConfigFactory
import frontend.RGBEventManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.util.Random

class SetCollectorTest extends AnyWordSpec
  with BeforeAndAfterAll
  with Matchers {
  val config = ConfigFactory.parseString(
    """
          conviva.max-records = 100000
          akka.log-config-on-start = on
    """
  ).withFallback(ConfigFactory.load())
  val testKit = ActorTestKit("test", config)

  "Given R, G, B messages" must {
    val actor = testKit.spawn(SetCollector(), "rgbCollector-1")
    val epoch = Instant.now().toEpochMilli

    "For single R, place it internal state" in {
      //val actor = testKit.spawn(SetCollector(), "rgbCollector-1")
      val probe = testKit.createTestProbe[SetCollector.State]()
      val expectedState = State(Vector(RGB_Set(Red("red", epoch), None, None)), Vector.empty, 0, 0, 0, 100000)
      actor ! SetCollector.Red("red", epoch)
      actor ! SetCollector.GetState(probe.ref)
      probe.expectMessage(expectedState)
    }
    "For valid R, G, B, Form a valid set" in {
      //val actor = testKit.spawn(SetCollector(), "rgbCollector-2")
      val probe = testKit.createTestProbe[SetCollector.State]()
      val expectedState = State(
        Vector(RGB_Set(Red("red", epoch), Some(Green("green", epoch + 1)), Some(Blue("blue", epoch + 2)))),
        Vector.empty,
        1,
        1,
        1,
        100000
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
      assert(state.validSets == 10_000)
    }
  }

  "Given out of order of R, G, B" must {
    val actor = testKit.spawn(SetCollector(), "rgbCollector-3")
    val epoch = Instant.now().toEpochMilli

    "For First G, B, R with valid timestamps, place it internal state" in {
      val probe = testKit.createTestProbe[SetCollector.State]()
      actor ! SetCollector.Green("Green", epoch + 1)
      actor ! SetCollector.Blue("Blue", epoch + 2)
      actor ! SetCollector.Red("Red", epoch)
      actor ! SetCollector.GetState(probe.ref)

      val state: SetCollector.State = probe.receiveMessage(3.seconds)
      assert(state.validSets == 1)
    }

    "For First G, B, R with invalid timestamps, place it internal state" in {
      val probe = testKit.createTestProbe[SetCollector.State]()
      actor ! SetCollector.Green("Green", epoch)
      actor ! SetCollector.Blue("Blue", epoch + 1)
      actor ! SetCollector.Red("Red", epoch + 2)
      actor ! SetCollector.GetState(probe.ref)

      val state: SetCollector.State = probe.receiveMessage(3.seconds)
      assert(state.validSets == 1)
    }

  }

  "Given R, G, B batch messages" must {
    val actor = testKit.spawn(SetCollector(), "rgbCollector-4")
    val epoch = Instant.now().toEpochMilli
    val reply = testKit.createTestProbe[RGBEventManager.ManagerCommand]()

    "For single R batch message, place it internal state" in {
      val probe = testKit.createTestProbe[SetCollector.State]()
      val redBatch = (1 to 100).map { i => Red(s"red-$i", epoch + i) }
      actor ! SetCollector.BatchedCommand(redBatch, reply.ref)
      actor ! SetCollector.GetState(probe.ref)
      val result: State = probe.receiveMessage(3.seconds)
      assert(result.list.size == 100)
    }

    "For single Blue batch message, place it internal state" in {
      val probe = testKit.createTestProbe[SetCollector.State]()
      val blueBatch = (1 to 100).map { i => Blue(s"blue-$i", epoch + i + 2) }
      actor ! SetCollector.BatchedCommand(blueBatch, reply.ref)
      actor ! SetCollector.GetState(probe.ref)
      val result: State = probe.receiveMessage(3.seconds)
      assert(result.list.size == 100)
      assert(result.outOfOrderMessages.size == 100)
    }
    "For single Green batch message, place it internal state" in {
      val probe = testKit.createTestProbe[SetCollector.State]()
      val greenBatch = (1 to 100).map { i => Green(s"green-$i", epoch + i + 1) }
      actor ! SetCollector.BatchedCommand(greenBatch, reply.ref)
      actor ! SetCollector.GetState(probe.ref)
      val result: State = probe.receiveMessage(3.seconds)
      assert(result.list.size == 100)
      assert(result.validSets == 100)
      assert(result.outOfOrderMessages.isEmpty)
    }
    // query


  }

  "Given start, end time" must {
    val actor = testKit.spawn(SetCollector(), "rgbCollector-5")
    val epoch = Instant.now().toEpochMilli
    val reply = testKit.createTestProbe[RGBEventManager.ManagerCommand]()
    val probe = testKit.createTestProbe[List[RGB_Set]]()
    "return all sets between given time" in {
      // load 1000 sets each with interval of 10 ms
      // R T+10, G T+11 B T+12 etc
      val redBatch = (1 to 1000).map { i => Red(s"red-$i", epoch + (i * 10)) }
      val blueBatch = (1 to 1000).map { i => Blue(s"red-$i", epoch + (i * 10) + 2) }
      val greenBatch = (1 to 1000).map { i => Green(s"red-$i", epoch + (i * 10) + 1) }
      // send to actor
      actor ! SetCollector.BatchedCommand(redBatch, reply.ref)
      actor ! SetCollector.BatchedCommand(blueBatch, reply.ref)
      actor ! SetCollector.BatchedCommand(greenBatch, reply.ref)
      // query time : lets get 500 to 600 => T+5000 to T+6000
      actor ! SetCollector.Query(epoch + 5000, epoch + 6000, probe.ref)
      val res: Seq[RGB_Set] = probe.receiveMessage(10.seconds)
      assert(res.size == 94)
    }
  }


  override def afterAll(): Unit = testKit.shutdownTestKit()

}
