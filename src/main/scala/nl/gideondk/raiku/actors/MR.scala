package nl.gideondk.raiku.actors

import java.net.InetSocketAddress

import com.basho.riak.protobuf._

import akka.actor.actorRef2Scala
import akka.io.Tcp.Write
import akka.util.ByteString

import nl.gideondk.raiku.commands._

import nl.gideondk.raiku.serialization.ProtoBufConversion
import play.api.libs.iteratee.Concurrent
import spray.json._

private class RaikuMRWorkerActor(addr: InetSocketAddress) extends RaikuPBActor {
  val workerDescription = "Raiku client MR worker"
  val address = addr

  override def specificMessageHandler = {
    case req: RiakMROperation ⇒
      tcpWorker match {
        case None ⇒ stash()
        case Some(w) ⇒
          req.promise.success()
          val iteratees = for (i ← 0 to req.channels.length - 1) yield {
            val channel = req.channels(i)
            MRIteratees.readFrame flatMap { tpl ⇒
              val msgType = tpl._1
              val frame = tpl._2
              MRIteratees.handlePhaseFrame(msgType, frame, req.channels, i, None)
            }
          }

          for {
            _ ← state
            _ ← iteratees.foldLeft(akka.actor.IO.Iteratee())((a, b) ⇒ a.flatMap(_ ⇒ b))
          } yield ()
          w ! Write(req.command)
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

  def readFrame = for {
    frameLenBytes ← akka.actor.IO.take(4) // First 32 bits
    frameLen = frameLenBytes.iterator.getInt - 1
    messageType ← akka.actor.IO.take(1) // Second 8 bits
    frame ← akka.actor.IO.take(frameLen)
  } yield {
    (messageType.asByteBuffer.get.toInt, frame)
  }

  def handleDoneFrame = for {
    done ← akka.actor.IO.Iteratee()
  } yield ()

  def handlePhaseFrame(messageType: Int, frame: ByteString, channels: List[Concurrent.Channel[JsValue]], currChannelIdx: Int, currentPhase: Option[Int]): akka.actor.IO.Iteratee[Unit] = for {
    z ← akka.actor.IO.Iteratee()
    next ← RiakMessageType.intToMessageType(messageType) match {
      case RiakMessageType.RpbErrorResp ⇒
        val exception = new Exception(RpbErrorResp().mergeFrom(frame.toArray).errmsg)
        channels.foreach(channel ⇒ channel.end(exception))
        throw exception

      case RiakMessageType.RpbMapRedResp ⇒
        val channel = channels(currChannelIdx)
        val mrResp = RpbMapRedResp().mergeFrom(frame.toArray)
        try {
          if (mrResp.done.isDefined) {
            channel.eofAndEnd()
            handleDoneFrame
          }
          else if (currentPhase.isDefined && currentPhase.get == mrResp.phase.get) {
            channel push mrRespToJsValue(mrResp)
            readFrame flatMap { tpl ⇒
              val msgType = tpl._1
              val frame = tpl._2
              handlePhaseFrame(msgType, frame, channels, currChannelIdx, currentPhase)
            }
          }
          else if (currentPhase.isDefined && currentPhase.get != mrResp.phase.get) {
            channel.eofAndEnd()
            val newIndex = currChannelIdx + 1
            if (channels.length >= newIndex)
              channels(newIndex) push mrRespToJsValue(mrResp)
            akka.actor.IO.Iteratee()
          }
          else {
            val channel = channels(currChannelIdx)
            channel push mrRespToJsValue(mrResp)
            readFrame flatMap { tpl ⇒
              val msgType = tpl._1
              val frame = tpl._2
              handlePhaseFrame(msgType, frame, channels, currChannelIdx, Some(mrResp.phase.get))
            }
          }
        }
        catch {
          case e: Exception ⇒
            val channel = channels(currChannelIdx)
            channel end (new Exception("Couldn't parse map reduce response."))
            akka.actor.IO.Iteratee()
        }

      case _ ⇒
        channels.foreach(channel ⇒ channel end (new Exception("Received unexepected message type")))
        throw new Exception("Received unexepected message type")
    }
  } yield ()
}

