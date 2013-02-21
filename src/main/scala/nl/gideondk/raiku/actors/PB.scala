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

  def workerDescription: String

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
      log.debug(workerDescription+" connected to "+remoteAddr)

    case ErrorClosed(cause) ⇒
      log.error(workerDescription+" disconnected from Riak ("+address+") with cause: "+cause)
      throw new WorkerDisconnectedUnexpectedlyException

    case PeerClosed ⇒
      log.error(workerDescription+" disconnected from Riak ("+address+")")
      throw new WorkerDisconnectedUnexpectedlyException

    case ConfirmedClosed ⇒
      log.debug(workerDescription+" disconnected from Riak ("+address+")")

    case m: ConnectionClosed ⇒
      log.error(workerDescription+" disconnected from Riak ("+address+")") // TODO: handle the specific cases
      throw new WorkerDisconnectedUnexpectedlyException

    case Received(bytes: ByteString) ⇒
      state(akka.actor.IO.Chunk(bytes))
  }

  def receive = specificMessageHandler orElse genericMessageHandler
}
