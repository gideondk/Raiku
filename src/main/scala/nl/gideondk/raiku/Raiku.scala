package nl.gideondk.raiku

import actors._
import actors.RaikuConfig
import actors.RaikuHost
import commands.{ MapReduce, RWRequests }
import akka.actor._

case class RaikuClient(config: RaikuConfig)(implicit val system: ActorSystem) extends RWRequests with MapReduce {
  val actor = system.actorOf(Props(new RaikuActor(config)))

  def disconnect = {
    system stop actor
  }
}

object RaikuClient {
  def apply(host: String, port: Int, connections: Int = 6, mrConnections: Int = 2)(implicit system: ActorSystem): RaikuClient = {
    val client = RaikuClient(RaikuConfig(RaikuHost(host, port), connections, mrConnections))
    client.actor ! InitializeRouters
    client
  }
}
