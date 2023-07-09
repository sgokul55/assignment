package frontend.api.domain

import backend.SetCollector
import spray.json.DefaultJsonProtocol._

object StatusProtocol {

  implicit val RedFormat = jsonFormat2(SetCollector.Red)
  implicit val greenFormat = jsonFormat2(SetCollector.Green)
  implicit val blueFormat = jsonFormat2(SetCollector.Blue)
  implicit val setCollectorFormat = jsonFormat3(SetCollector.RGB_Set)
  implicit val setRespFormat = jsonFormat1(SetsResponse)


  case class SetsResponse(sets: List[SetCollector.RGB_Set])
  case class Status(validSetCount: Int, outOfOrderMessagesCount: Int, setSize: Int)

  case class Stat(data: Seq[Status])

}
