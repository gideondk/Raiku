package nl.gideondk.raiku.actors

import scalaz._
import Scalaz._

import akka.actor.IO.IterateeRef
import akka.actor._
import akka.util.ByteString
import akka.event.Logging

import akka.io._

import scala.concurrent.Promise

import java.net.InetSocketAddress

import akka.actor.SupervisorStrategy._
import akka.routing.RandomRouter
import akka.actor.OneForOneStrategy

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import akka.io.Tcp._

private[raiku] case class RiakResponse(length: Int, messageType: Int, message: ByteString)

private[raiku] case class RiakOperation(promise: Promise[RiakResponse], bytes: ByteString)

case class RaikuHost(host: String, port: Int)

case class RaikuConfig(host: RaikuHost, connections: Int, reconnectDelay: FiniteDuration = 5 seconds)

object RaikuActor {
  val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2) {
      case _ ⇒ Restart
    }
}

private[raiku] class RaikuActor(config: RaikuConfig) extends Actor {
  val log = Logging(context.system, this)
  val address = new InetSocketAddress(config.host.host, config.host.port)

  val tcp = akka.io.IO(Tcp)(context.system)
  var router: Option[ActorRef] = None

  def initialize {
    router = Some(context.system.actorOf(Props(new RaikuWorkerActor(tcp, address))
      .withRouter(RandomRouter(nrOfInstances = config.connections, supervisorStrategy = RaikuActor.supervisorStrategy))
      .withDispatcher("akka.actor.raiku-dispatcher")))
    context.watch(router.get)
  }

  def receive = {
    case InitializeRouter ⇒
      log.debug("Raiku router initializing")
      initialize

    case ReconnectRouter ⇒
      if (router.isEmpty) initialize

    case Terminated(name) ⇒
      router = None
      log.debug("Raiku router died, restarting in: "+config.reconnectDelay.toString())
      context.system.scheduler.scheduleOnce(config.reconnectDelay, self, ReconnectRouter)

    case req @ RiakOperation(promise, command) ⇒
      router match {
        case Some(r) ⇒ r forward req
        case None    ⇒ promise.failure(NoConnectionException())
      }
  }
}

private class RaikuWorkerActor(tcp: ActorRef, address: InetSocketAddress) extends Actor with Stash {
  val state = IterateeRef.async
  var tcpWorker: Option[ActorRef] = None
  val log = Logging(context.system, this)

  override def preStart = {
    tcp ! Connect(address)
  }

  override def postStop = {
    tcp ! Close
    state(akka.actor.IO.EOF)
  }

  def receive = {
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

    case req @ RiakOperation(promise, command) ⇒
      tcpWorker match {
        case None ⇒ stash()
        case Some(w) ⇒
          for {
            _ ← state
            result ← Iteratees.readRiakResponse
          } yield {
            promise.success(result)
            result
          }
          w ! Write(command)
      }
  }
}

object Iteratees {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def readRiakResponse = for {
    frameLenBytes ← akka.actor.IO.take(4) // First 32 bits
    frameLen = frameLenBytes.iterator.getInt - 1
    messageType ← akka.actor.IO.take(1) // Second 8 bits
    frame ← akka.actor.IO.take(frameLen)
  } yield {
    RiakResponse(frameLen + 1, messageType.head.toInt, frame)
  }
}

trait WorkerDisconnectedException extends Exception

case class WorkerDisconnectedUnexpectedlyException extends WorkerDisconnectedException

case class WorkerDisconnectedExpectedly extends WorkerDisconnectedException

case class NoConnectionException extends Exception

case object InitializeRouter

case object ReconnectRouter

