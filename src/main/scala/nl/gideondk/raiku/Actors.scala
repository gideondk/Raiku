package nl.gideondk.raiku

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
import akka.routing.RoundRobinRouter
import akka.io.Tcp._
import akka.io.Tcp.Connected
import akka.io.Tcp.Register
import akka.io.Tcp.Connect
import akka.actor.OneForOneStrategy
import akka.io.Tcp.ErrorClosed
import akka.io.Tcp.Received

case class RaikuHost(host: String, port: Int)

case class RaikuConfig(host: RaikuHost, connections: Int)

// private[raiku] class RaikuActor(config: RaikuConfig) extends Actor {
//   val log = Logging(context.system, this)
//   val state = IterateeRef.Map.async[IO.Handle]()(context.dispatcher)

//   var sockets: Vector[IO.SocketHandle] = _
//   var nextSocketIndex: Int = 0
//   var nextHostIndex: Int = 0

//   val reconnectDelay = 1 second
//   var reconnectRetries = 5
//   var reconnectResetDelay = 30 seconds

//   var requests = 0L

//   var operations: Map[SocketHandle, List[RiakOperation]] = Map.empty
//   val queued = Queue.empty[RiakOperation]

//   def connections = config.connections

//   def nextHost = {
//     val host = config.hosts(nextHostIndex)
//     nextHostIndex = (nextHostIndex + 1) % config.hosts.length
//     host
//   }

//   override def preStart = {
//     log info ("Connecting")
//     sockets = Vector.fill(connections) {
//       val host = nextHost
//       IOManager(context.system).connect(host.host, host.port)
//     }
//   }

//   override def postStop() {
//     log info ("Shutting down")
//     sockets.foreach(_.close)
//   }

//   def removeSocket(socket: IO.SocketHandle) = {
//     operations.get(socket) match {
//       case Some(reqs) ⇒ reqs.foreach(x ⇒ if (!x.promise.isCompleted) x.promise failure new RuntimeException("Socket closed during excecution"))
//       case _          ⇒
//     }
//     operations = operations - socket
//     sockets = sockets diff Vector(socket)

//     if (sockets.length == 0) {
//       log info ("All sockets closed, retrying in "+reconnectDelay.toSeconds+" second(s)")
//       context.system.scheduler.scheduleOnce(reconnectDelay, self, Reconnect)
//     }
//   }

//   def nextSocket = {
//     if (sockets.length > 0) {
//       if (nextSocketIndex > sockets.length) {
//         nextSocketIndex = 0
//       }
//       val socket = sockets(nextSocketIndex)
//       nextSocketIndex = (nextSocketIndex + 1) % sockets.length
//       socket.point[Option]
//     }
//     else None
//   }

//   def checkAndReinitSockets = if (sockets.length < math.ceil(config.connections / 2)) {
//     val nrDiff = config.connections - sockets.length
//     sockets = sockets ++ Vector.fill(nrDiff) {
//       val host = nextHost
//       IOManager(context.system).connect(host.host, host.port)
//     }
//   }

//   def receive = {
//     case IO.Connected(socket, address) ⇒
//       log.info("Successfully connected to "+address)
//       if (queued.length > 0) {
//         log.info("Starting"+queued.length+" items in queue")
//         queued.dequeueAll(x ⇒ true) foreach (self !)
//       }

//     case IO.Read(socket, bytes) ⇒
//       state(socket)(IO Chunk bytes)

//     case IO.Closed(socket: IO.SocketHandle, cause) ⇒ {
//       removeSocket(socket)
//       log.info("Socket has closed, cause: "+cause)
//     }

//     case Reconnect ⇒ {
//       if (reconnectRetries == 0) {
//         reconnectRetries = 5
//         context.system.scheduler.scheduleOnce(reconnectResetDelay, self, Reconnect)
//       }
//       else {
//         reconnectRetries = reconnectRetries - 1
//         sockets = Vector.fill(connections) {
//           val host = nextHost
//           IOManager(context.system).connect(host.host, host.port)
//         }
//       }
//     }

//     case fo @ FinishOperation(s, ro) ⇒
//       operations = operations.get(s) match {
//         case Some(l) ⇒ operations ++ Map(s -> (l diff List(ro)))
//         case None    ⇒ operations
//       }

//     case req @ RiakOperation(promise, bytes) ⇒
//       nextSocket match {
//         case Some(socket) ⇒
//           operations = operations ++ Map(socket -> (~operations.get(socket) ++ List(req)))
//           socket write bytes
//           onRequest(req)

//           for {
//             _ ← state(socket)
//             result ← Iteratees.readRiakResponse
//           } yield {
//             self ! FinishOperation(socket, req)
//             promise success result
//           }

//         case None ⇒
//           queued enqueue req
//         //promise failure new Exception("No sockets available")
//       }

//   }

//   def onRequest(req: RiakOperation): Unit = {
//     requests += 1L
//   }
// }

//InitializeRaikuClientActor(init: Promise[Unit])

object RaikuActor {
  val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2) {
      case _: RaikuWorkerDisconnectedUnexpectedlyException ⇒ Restart
      case _: RaikuWorkerNoTCPException ⇒ restart
      case _: NullPointerException ⇒ restart
      case _ ⇒ Escalate
    }
}

private class RaikuActor(config: RaikuConfig) extends Actor {
  val log = Logging(context.system, this)
  val address = new InetSocketAddress(config.host.host, config.host.port)

  val tcp = akka.io.IO(Tcp)(context.system)
  val router = context.system.actorOf(Props(new RaikuWorkerActor(tcp, address)).withRouter(RoundRobinRouter(1)))

  def receive = {
    case req @ RiakOperation(promise, command) ⇒
      router forward req
  }
}

private class RaikuWorkerActor(tcp: ActorRef, address: InetSocketAddress) extends Actor {
  val state = IterateeRef.async(Iteratees.readRiakResponse)(context.dispatcher)
  var tcpWorker: ActorRef = _
  val log = Logging(context.system, this)

  //override def preStart = tcp ! Connect(address)

  def receive = {
    case Connected(remoteAddr, localAddr) ⇒
      sender ! Register(self)
      tcpWorker = sender
      log.debug("Raiku client worker connected to "+remoteAddr)

    case ErrorClosed(cause) ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+") with cause: "+cause)
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case PeerClosed ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+")")
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case ConfirmedClosed ⇒
      log.debug("Raiku client worker disconnected from Riak ("+address+")")

    case m: ConnectionClosed ⇒
      log.error("Raiku client worker disconnected from Riak ("+address+")") // TODO: handle the specific cases
      throw new RaikuWorkerDisconnectedUnexpectedlyException

    case Received(bytes: ByteString) ⇒
      state(akka.actor.IO.Chunk(bytes))

    case req @ RiakOperation(promise, command) ⇒
      tcpWorker ! Write(command)

      for {
        _ ← state
        result ← Iteratees.readRiakResponse
      } yield {
        promise success result
        result
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

private[raiku] case class RiakResponse(length: Int, messageType: Int, message: ByteString)

private[raiku] case class RiakOperation(promise: Promise[RiakResponse], bytes: ByteString)

trait RaikuWorkerDisconnectedException extends Exception

case class RaikuWorkerDisconnectedUnexpectedlyException extends RaikuWorkerDisconnectedException

case class RaikuWorkerDisconnectedExpectedly extends RaikuWorkerDisconnectedException

case class RaikuWorkerNoTCPException extends Exception

// private[raiku] case class FinishOperation(socket: SocketHandle, op: RiakOperation)

// private[raiku] case object Reconnect