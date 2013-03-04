package nl.gideondk.raiku.actors

import java.net.InetSocketAddress

import scala.collection.mutable.Queue

import scalaz._
import Scalaz._

import akka.actor.IO.IterateeRef
import akka.actor._
import akka.util.ByteString
import akka.event.Logging

import akka.io._
import akka.io.Tcp._

trait RaikuPBActor extends Actor with Stash {
  import context.dispatcher

  def address: InetSocketAddress

  def workerDescription: String

  val state = IterateeRef.async
  var tcpWorker: Option[ActorRef] = None
  val log = Logging(context.system, this)

  val tcp = akka.io.IO(Tcp)(context.system)

  val writeQueue = Queue[ByteString]()
  var writeAvailable = true

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
      log.debug(workerDescription+" disconnected from Riak ("+address+") with cause: "+cause)
      throw new WorkerDisconnectedUnexpectedlyException

    case PeerClosed ⇒
      log.debug(workerDescription+" disconnected from Riak ("+address+")")
      throw new WorkerDisconnectedUnexpectedlyException

    case ConfirmedClosed ⇒
      log.debug(workerDescription+" disconnected from Riak ("+address+")")

    case m: ConnectionClosed ⇒
      log.debug(workerDescription+" disconnected from Riak ("+address+")") // TODO: handle the specific cases
      throw new WorkerDisconnectedUnexpectedlyException

    case Received(bytes: ByteString) ⇒
      state(akka.actor.IO.Chunk(bytes))

    case CommandFailed(cmd: Command) ⇒
    // TODO: Implement retrier functionality

    case WriteAck ⇒
      writeAvailable = true
      if (writeQueue.length > 0)
        write(writeQueue.dequeue())
  }

  def write(bs: ByteString) = {
    if (writeAvailable) {
      tcpWorker match {
        case None ⇒ throw new Exception("Trying to write to a undefined worker")
        case Some(w) ⇒
          writeAvailable = false
          w ! Write(bs, WriteAck)
      }
    }
    else {
      writeQueue enqueue bs
    }
  }

  def receive = specificMessageHandler orElse genericMessageHandler
}

case object WriteAck
