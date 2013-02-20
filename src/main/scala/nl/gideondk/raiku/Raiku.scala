package nl.gideondk.raiku

import akka.actor.ActorSystem
import akka.actor.Props
import com.basho.riak.protobuf.RiakPB._
import com.basho.riak.protobuf.RiakKvPB._
import com.basho.riak.protobuf.RpbGetServerInfoResp
import akka.util.ByteString
import scala.concurrent.Promise
import scala.concurrent.Future
import com.basho.riak.protobuf._
import akka.actor._
import akka.routing._

import scala.concurrent._
import scala.concurrent.duration._

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
