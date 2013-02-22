package nl.gideondk.raiku.actors

import akka.actor._
import akka.util.ByteString

import spray.json._

import java.net.InetSocketAddress

import akka.io.Tcp._
import play.api.libs.iteratee._
import nl.gideondk.raiku.commands.{ RiakMROperation, RiakMessageType }
import com.basho.riak.protobuf.{ RpbMapRedResp, RpbErrorResp }
import nl.gideondk.raiku.serialization.ProtoBufConversion

private class RaikuMRWorkerActor(addr: InetSocketAddress) extends RaikuPBActor {
  val workerDescription = "Raiku client MR worker"
  val address = addr

  override def specificMessageHandler = {
    case req @ RiakMROperation(promise, command) ⇒
      tcpWorker match {
        case None ⇒ stash()
        case Some(w) ⇒
          val (phaseEnum, phaseChannel) = Concurrent.broadcast[Enumerator[JsValue]]
          promise.success(phaseEnum)
          state flatMap {
            _ ⇒ MRIteratees.readMRResponse(phaseChannel)
          }
          w ! Write(command)
      }
  }
}

trait MRConversion extends ProtoBufConversion {
  def mrRespToJsValue(resp: RpbMapRedResp) = {
    JsonParser(resp.response.get).asInstanceOf[JsArray].elements.head //TODO: Less assumptions!
  }
}

object MRIteratees extends MRConversion {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  def readMRResponse(phaseEnumeratorChannel: Concurrent.Channel[Enumerator[JsValue]],
                     currentResultEnumeratorChannel: Option[Concurrent.Channel[JsValue]] = None,
                     currentPhase: Int = 0): akka.actor.IO.Iteratee[Unit] = for {
    frameLenBytes ← akka.actor.IO.take(4) // First 32 bits
    frameLen = frameLenBytes.iterator.getInt - 1
    messageType ← akka.actor.IO.take(1) // Second 8 bits
    frame ← akka.actor.IO.take(frameLen)
    next ← handleMRResult(messageType.asByteBuffer.get.toInt, frame, phaseEnumeratorChannel, currentResultEnumeratorChannel)
  } yield {
    ()
  }

  def handleMRResult(messageType: Int,
                     frame: ByteString,
                     phaseEnumeratorChannel: Concurrent.Channel[Enumerator[JsValue]],
                     currentResultEnumeratorChannel: Option[Concurrent.Channel[JsValue]] = None,
                     currentPhase: Int = 0) = RiakMessageType.intToMessageType(messageType) match {
    case RiakMessageType.RpbErrorResp ⇒
      phaseEnumeratorChannel.end(new Exception(RpbErrorResp().mergeFrom(frame.toArray).errmsg))
      akka.actor.IO.Iteratee()

    case RiakMessageType.RpbMapRedResp ⇒
      val mrResp = RpbMapRedResp().mergeFrom(frame.toArray)
      mrResp.done.getOrElse(false) match {
        case false ⇒ {
          try {
            val mrResultChannel = {
              if (currentResultEnumeratorChannel.isDefined) {
                currentResultEnumeratorChannel.get
              }
              else {
                val (resultEnum, resultChannel) = Concurrent.broadcast[JsValue]
                phaseEnumeratorChannel push resultEnum
                resultChannel
              } // Initial
            }

            if (currentPhase == mrResp.phase.get) {
              mrResultChannel push mrRespToJsValue(mrResp)
              readMRResponse(phaseEnumeratorChannel, Some(mrResultChannel), currentPhase)
            }
            else {
              mrResultChannel.eofAndEnd()
              val (resultEnum, resultChannel) = Concurrent.broadcast[JsValue]
              phaseEnumeratorChannel push resultEnum
              resultChannel push mrRespToJsValue(mrResp)

              readMRResponse(phaseEnumeratorChannel, Some(resultChannel), mrResp.phase.get)
            }
          }
          catch {
            case e: Exception ⇒
              phaseEnumeratorChannel end (new Exception("Couldn't parse map reduce response."))
              akka.actor.IO.Iteratee()
          }
        }
        case true ⇒
          if (currentResultEnumeratorChannel.isDefined) currentResultEnumeratorChannel.get.eofAndEnd()
          phaseEnumeratorChannel.eofAndEnd()
          akka.actor.IO.Iteratee()
      }

    case _ ⇒
      phaseEnumeratorChannel end (new Exception("Received unexepected message type"))
      akka.actor.IO.Iteratee()
  }

}

