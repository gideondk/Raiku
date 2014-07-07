package nl.gideondk.raiku

import actors._
import commands._
import akka.actor._

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

case class RaikuHost(host: String, port: Int)

case class RaikuConfig(host: RaikuHost, connections: Int, mrConnections: Int, streamConnections: Int, reconnectDelay: FiniteDuration = 2 seconds)

case class RaikuClient(config: RaikuConfig)(implicit val system: ActorSystem) extends GeneralRequests with RawRequests with RaikuValueRequests with BucketRequests with IndexRequests with CounterRequests {
  val worker = RaikuWorker(config.host.host, config.host.port, config.connections)
  val streamWorker = RaikuStreamWorker(config.host.host, config.host.port, config.streamConnections)

  def disconnect = {
    system stop worker.actor
    system stop streamWorker.actor
  }
}

object RaikuClient {
  def apply(host: String, port: Int, connections: Int = 12, mrConnections: Int = 2, streamConnections: Int = 3)(implicit system: ActorSystem): RaikuClient = {
    val client = RaikuClient(RaikuConfig(RaikuHost(host, port), connections, mrConnections, streamConnections))
    client
  }
}
