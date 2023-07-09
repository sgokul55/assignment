package frontend.api

import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import frontend.RGBEventManager
import frontend.api.domain.StatusProtocol._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class AggregationRoutes(manager: ActorRef[RGBEventManager.ManagerCommand])(implicit system: ActorSystem[_]) {


  implicit val timeout: Timeout = 10.seconds

  implicit val ec = system.executionContext
  val routes: Route =
    pathPrefix("events") {
      concat(
        path("get-sets") {
          get {
            parameters(
              Symbol("startTime").as[Long],
              Symbol("endTime").as[Long]
            ) { (s, e) =>
              complete(queryForSets(s, e))
            }
          }
        },
        path("get-stats") {
          get {
            complete(getStatus())
          }
        }
      )
    }

  private def queryForSets(startTime: Long, endTime: Long): Future[SetsResponse] =
    manager
      .ask(RGBEventManager.Query(startTime, endTime, _)).map(SetsResponse)

  private def getStatus(): Future[Stat] = manager.ask(RGBEventManager.GetStatus)
}
