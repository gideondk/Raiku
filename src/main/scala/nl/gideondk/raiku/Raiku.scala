package nl.gideondk.raiku

import actors._
import commands._
import akka.actor._

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

case class RaikuHost(host: String, port: Int)

case class RaikuConfig(host: RaikuHost, connections: Int, mrConnections: Int, streamConnections: Int, reconnectDelay: FiniteDuration = 2 seconds)

case class RaikuClient(config: RaikuConfig)(implicit val system: ActorSystem) extends GeneralRequests with RWRequests with BucketRequests with IndexRequests with CounterRequests with MapReduce {
  val worker = RaikuWorker(config.host.host, config.host.port, config.connections)
  //  val mrWorker = RaikuMRWorker(config.host.host, config.host.port, config.mrConnections)
  //  val siWorker = Raiku2iStreamWorker(config.host.host, config.host.port, config.streamConnections)

  def disconnect = {
    system stop worker.actor
    //    system stop mrWorker.actor
    //    system stop siWorker.actor
  }
}

object RaikuClient {
  def apply(host: String, port: Int, connections: Int = 12, mrConnections: Int = 2, streamConnections: Int = 3)(implicit system: ActorSystem): RaikuClient = {
    val client = RaikuClient(RaikuConfig(RaikuHost(host, port), connections, mrConnections, streamConnections))
    client
  }
}
