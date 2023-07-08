package backend

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import com.typesafe.config.ConfigFactory
import frontend.RGBEventManager
import backend.SetCollector._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class SetCollectorRetireTest extends AnyWordSpec
  with BeforeAndAfterAll
  with Matchers {

  val config = ConfigFactory.parseString(
    """
          conviva.max-records = 10
          akka.log-config-on-start = on
    """
  ).withFallback(ConfigFactory.load())
  val testKit = ActorTestKit("test", config)

  "After aging, Set collector actor" must {
    val probe = testKit.createTestProbe[RGBEventManager.ManagerCommand]()
    val actor = testKit.spawn(SetCollector(), "rgbCollector-4")
    "Expect retirement request upon max record reached" in {
      val epoch = Instant.now().toEpochMilli
      val redBatch = (1 to 11).map { i => Red(s"red-$i", epoch + i) }
      val blueBatch = (1 to 11).map { i => Blue(s"blue-$i", epoch + i + 2) }
      val greenBatch = (1 to 11).map { i => Green(s"green-$i", epoch + i + 1) }
      actor ! SetCollector.BatchedCommand(redBatch, probe.ref)
      actor ! SetCollector.BatchedCommand(blueBatch, probe.ref)
      actor ! SetCollector.BatchedCommand(greenBatch, probe.ref)

      probe.expectMessage(RGBEventManager.RetirementRequest(Instant.ofEpochMilli(epoch + 1), Instant.ofEpochMilli(epoch + 11), actor))
    }
    "Return the events given after retirement" in {
      val epoch = Instant.now().toEpochMilli
      val redBatch = (1 to 11).map { i => Red(s"red-$i", epoch + i) }
      actor ! SetCollector.BatchedCommand(redBatch, probe.ref)
      probe.expectMessage(RGBEventManager.ReturnUnprocessed(redBatch))
    }
  }

  override def afterAll(): Unit = testKit.shutdownTestKit()

}
