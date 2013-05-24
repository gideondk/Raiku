package nl.gideondk.raiku

import actors._
import actors.RaikuConfig
import actors.RaikuHost
import commands._
import akka.actor._

case class RaikuClient(config: RaikuConfig)(implicit val system: ActorSystem) extends GeneralRequests with RWRequests with BucketRequests with IndexRequests with MapReduce {
  val worker = RaikuWorker(config.host.host, config.host.port, config.connections)
  val mrWorker = RaikuMRWorker(config.host.host, config.host.port, config.mrConnections)

  def disconnect = {
    system stop worker
    system stop mrWorker
  }
}

object RaikuClient {
  def apply(host: String, port: Int, connections: Int = 4, mrConnections: Int = 2)(implicit system: ActorSystem): RaikuClient = {
    val client = RaikuClient(RaikuConfig(RaikuHost(host, port), connections, mrConnections))
    client
  }
}
