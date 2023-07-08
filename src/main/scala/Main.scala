import akka.NotUsed
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorSystem, Behavior, Terminated}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import frontend.{KafkaConsumer, RGBEventManager}

import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
    ActorSystem(Main(), "conviva-event-collector")
  }

  def apply(): Behavior[NotUsed] =
    Behaviors.setup { context =>
      implicit val system = context.system
      val manager = context.spawn(RGBEventManager.apply(), "event-manager")
      val kafkaConsumer = new KafkaConsumer(manager)
      //      val routes = new AggregationRoutes(manager)
      //      startHttpServer(routes.triggerRoutes)(context.system)
      Behaviors.receiveSignal { case (_, Terminated(_)) =>
        context.log.error("------- Executor Actor stopped! --------")
        Behaviors.stopped
      }
    }

  private def startHttpServer(
                               routes: Route
                             )(implicit system: ActorSystem[_]): Unit = {
    import system.executionContext
    val futureBinding = Http().newServerAt("0.0.0.0", 8080).bind(routes)
    futureBinding.onComplete {
      case Success(binding) =>
        val address = binding.localAddress
        system.log.info(
          "Server online at http://{}:{}/",
          address.getHostString,
          address.getPort
        )
      case Failure(ex) =>
        system.log.error("Failed to bind HTTP endpoint, terminating system", ex)
        system.terminate()
    }
  }
}