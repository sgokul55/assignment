package frontend

import akka.Done
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.kafka.{CommitterSettings, ConsumerMessage, ConsumerSettings, Subscriptions}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}

class KafkaConsumer(manager: ActorRef[RGBEventManager.ManagerCommand])(implicit actorSystem: ActorSystem[_]) {

  implicit val ec: ExecutionContextExecutor = actorSystem.executionContext
  val broker = actorSystem.settings.config.getString("kafka-broker")
  val group = actorSystem.settings.config.getString("kafka-group")
  val config = actorSystem.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings = ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
    .withGroupId(group)
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    .withBootstrapServers(broker)
  val topic = actorSystem.settings.config.getString("kafka-topic")

  val control: DrainingControl[Done] =
    Consumer
      .committableSource(consumerSettings, Subscriptions.topics(topic))
      .groupedWithin(1000, 3.seconds)
      .mapAsync(1) { msg =>
        sort(msg).map { i =>
          i.foreach(e => manager ! RGBEventManager.InputEvent(e))
          msg.map(_.committableOffset)
        }
      }
      .mapConcat(f => f)
      .toMat(Committer.sink(CommitterSettings.apply(actorSystem)))(DrainingControl.apply)
      .run()

  def sort(key: Seq[ConsumerMessage.CommittableMessage[String, String]]): Future[Seq[String]] = {
    Future {
      val a: Seq[String] = key.map(_.record.value())
      a.sorted
    }
  }
}
