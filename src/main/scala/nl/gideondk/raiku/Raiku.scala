package nl.gideondk.raiku

import akka.actor._
import akka.stream.ActorMaterializer
import nl.gideondk.raiku.commands._
import nl.gideondk.raiku.protocol._

case class RaikuHost(host: String, port: Int)

case class RaikuConfig(host: RaikuHost)

case class RaikuClient(config: RaikuConfig)(implicit val system: ActorSystem, mat: ActorMaterializer) extends GeneralRequests with RawRequests with RaikuValueRequests with BucketRequests with IndexRequests with CounterRequests {
  val worker = RaikuWorker(config.host.host, config.host.port)
  val streamWorker = RaikuStreamWorker(config.host.host, config.host.port)
}

object RaikuClient {
  def apply(host: String, port: Int)(implicit system: ActorSystem, mat: ActorMaterializer): RaikuClient = {
    val client = RaikuClient(RaikuConfig(RaikuHost(host, port)))
    client
  }
}
