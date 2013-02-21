package nl.gideondk.raiku.actors

import java.net.InetSocketAddress

import scalaz._
import Scalaz._

import akka.actor.IO.IterateeRef
import akka.actor._
import akka.util.ByteString
import akka.event.Logging

import akka.io._
import akka.io.Tcp._

import scala.concurrent.ExecutionContext.Implicits.global

trait RaikuPBActor extends Actor with Stash {
  def address: InetSocketAddress

  val state = IterateeRef.async
  var tcpWorker: Option[ActorRef] = None
  val log = Logging(context.system, this)

  val tcp = akka.io.IO(Tcp)(context.system)

  override def preStart = {
    tcp ! Connect(address)
  }

  override def postStop = {
    tcp ! Close
    state(akka.actor.IO.EOF)
  }

  def specificMessageHandler: Receive

  def genericMessageHandler: Receive = {
    case Connected(remoteAddr, localAddr) ⇒
      sender ! Register(self)
      tcpWorker = sender.point[Option]
      unstashAll()
      log.debug("Raiku client worker connected to "+remoteAddr)

    case ErrorClosed(cause) ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+") with cause: "+cause)
      throw new WorkerDisconnectedUnexpectedlyException

    case PeerClosed ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+")")
      throw new WorkerDisconnectedUnexpectedlyException

    case ConfirmedClosed ⇒
      log.debug("Raiku client worker disconnected from Riak ("+address+")")

    case m: ConnectionClosed ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+")") // TODO: handle the specific cases
      throw new WorkerDisconnectedUnexpectedlyException

    case Received(bytes: ByteString) ⇒
      state(akka.actor.IO.Chunk(bytes))
  }

  def receive = specificMessageHandler orElse genericMessageHandler
}
