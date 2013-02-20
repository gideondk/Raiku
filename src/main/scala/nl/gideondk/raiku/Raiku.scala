package nl.gideondk.raiku

import actors.{ InitializeRouter, RaikuActor, RaikuConfig, RaikuHost }
import commands.RWRequests
import akka.actor._

case class RaikuClient(config: RaikuConfig)(implicit val system: ActorSystem) extends RWRequests {
  val actor = system.actorOf(Props(new RaikuActor(config)))

  def disconnect = {
    system stop actor
  }
}

object RaikuClient {
  def apply(host: String, port: Int, connections: Int = 4)(implicit system: ActorSystem): RaikuClient = {
    val client = RaikuClient(RaikuConfig(RaikuHost(host, port), connections))
    client.actor ! InitializeRouter
    client
  }
}
