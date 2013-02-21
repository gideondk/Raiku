package nl.gideondk.raiku.actors

import akka.actor._
import akka.util.ByteString

import spray.json._

import scala.concurrent.Promise

import java.net.InetSocketAddress

import akka.io.Tcp._
import play.api.libs.iteratee._
import nl.gideondk.raiku.commands.{ RiakMROperation, RiakMessageType }
import com.basho.riak.protobuf.{ RpbMapRedResp, RpbErrorResp }
import nl.gideondk.raiku.serialization.ProtoBufConversion
import nl.gideondk.raiku.mapreduce.MRResult

private class RaikuMRWorkerActor(addr: InetSocketAddress) extends RaikuPBActor {
  val address = addr

  override def specificMessageHandler = {
    case req @ RiakMROperation(promise, command) ⇒
      tcpWorker match {
        case None ⇒ stash()
        case Some(w) ⇒
          val (resultEnum, resultChannel) = Concurrent.broadcast[MRResult]
          promise.success(resultEnum)
          state flatMap { _ ⇒ MRIteratees.readMRResponse(resultChannel) }
          w ! Write(command)
      }
  }
}

trait MRConversion extends ProtoBufConversion {
  def mrRespToMRResult(resp: RpbMapRedResp) = {
    MRResult(resp.phase.get, JsonParser(resp.response.get))
  }
}

object MRIteratees extends MRConversion {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def readMRResponse(channel: Concurrent.Channel[MRResult]): akka.actor.IO.Iteratee[Unit] = for {
    frameLenBytes ← akka.actor.IO.take(4) // First 32 bits
    frameLen = frameLenBytes.iterator.getInt - 1
    messageType ← akka.actor.IO.take(1) // Second 8 bits
    frame ← akka.actor.IO.take(frameLen)
    next ← {
      val a: akka.actor.IO.Iteratee[Unit] = RiakMessageType.intToMessageType(messageType.asByteBuffer.getInt) match {
        case RiakMessageType.RpbErrorResp ⇒
          channel.end(new Exception(RpbErrorResp().mergeFrom(frame.toArray).errmsg))
          akka.actor.IO.Iteratee()

        case RiakMessageType.RpbMapRedResp ⇒
          val mrResp = RpbMapRedResp().mergeFrom(frame.toArray)
          mrResp.done.getOrElse(false) match {
            case false ⇒ try {
              channel push mrRespToMRResult(mrResp)
              readMRResponse(channel)
            }
            catch {
              case e: Exception ⇒
                channel end (new Exception("Couldn't parse map reduce response."))
                akka.actor.IO.Iteratee()
            }
            case true ⇒
              channel.eofAndEnd()
              akka.actor.IO.Iteratee()
          }
        case _ ⇒
          channel end (new Exception("Received unexepected message type"))
          akka.actor.IO.Iteratee()
      }
      a
    }
  } yield {

    ()
  }
}

