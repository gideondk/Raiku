package nl.gideondk.raiku

import akka.actor.IO
import com.google.protobuf.MessageLite
import com.google.protobuf.CodedOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import akka.util.ByteString

import scalaz._
import Scalaz._

import com.google.protobuf.{ ByteString ⇒ ProtoBufByteString }
import com.basho.riak.protobuf._

trait ProtoBufConversion {
  implicit def stringToByteString(s: String): ProtoBufByteString = ProtoBufByteString.copyFromUtf8(s)
  implicit def byteStringToString(s: ProtoBufByteString): String = s.toStringUtf8

  implicit def byteStringToByteArray(s: ProtoBufByteString): Array[Byte] = s.toByteArray
  implicit def byteArrayToByteString(s: Array[Byte]): ProtoBufByteString = ProtoBufByteString.copyFrom(s)

  implicit def messageToByteArray[T <: MessageLite with MessageLite.Builder](m: com.basho.riak.protobuf.Message[T]) = {
    val os = new ByteArrayOutputStream()
    val cos = CodedOutputStream.newInstance(os)
    m.writeTo(cos)
    cos.flush
    os.close
    os.toByteArray()
  }

  def request(messageType: RiakMessageType): ByteString = {
    val bsb = ByteString.newBuilder
    val cos = new DataOutputStream(bsb.asOutputStream)
    cos.writeInt(1)
    cos.write(RiakMessageType.messageTypeToInt(messageType))
    cos.flush()
    cos.close()
    bsb.result
  }

  def request(messageType: RiakMessageType, message: Array[Byte]): ByteString = {
    val bsb = ByteString.newBuilder
    val cos = new DataOutputStream(bsb.asOutputStream)
    cos.writeInt(message.length + 1)
    cos.write(RiakMessageType.messageTypeToInt(messageType))
    cos.write(message)
    cos.flush()
    cos.close()
    bsb.result
  }

  def rwObjectToRpbContent(rw: RaikuRWObject) = { // TODO: implement links
    val indexes =
      rw.binIndexes.map(tpl ⇒ tpl._2.map(x ⇒ RpbPair(stringToByteString(tpl._1+"_bin"), stringToByteString(x).some))).flatten ++
        rw.intIndexes.map(tpl ⇒ tpl._2.map(x ⇒ RpbPair(stringToByteString(tpl._1+"_int"), stringToByteString(x.toString).some))).flatten

    RpbContent(rw.value, stringToByteString(rw.contentType).some, None, None, rw.vTag.map(x ⇒ byteArrayToByteString(x.v)), Vector[RpbLink](),
      rw.lastModified, rw.lastModifiedMicros, rw.userMeta.map(tpl ⇒ RpbPair(tpl._1, tpl._2.map(byteArrayToByteString(_)))).toVector, indexes.toVector, rw.deleted)
  }

  def pbContentToRWObject(key: String, bucket: String, c: RpbContent, vClock: Option[VClock]) = {
      def isBinIndex(idx: String) = idx.substring(idx.length - 4) == "_bin"
      def isIntIndex(idx: String) = idx.substring(idx.length - 4) == "_int"
      def indexName(idx: String) = idx.substring(0, idx.length - 4)

    val userMeta: Map[String, Option[Array[Byte]]] = c.usermeta.map(p ⇒ p.key.toStringUtf8 -> p.value.map(_.toByteArray)).toMap

    val indexes: Map[String, List[Option[com.google.protobuf.ByteString]]] =
      c.indexes.foldLeft(Map[String, List[Option[com.google.protobuf.ByteString]]]())((m, p) ⇒ m + (p.key.toStringUtf8 -> (~m.get(p.key.toStringUtf8) ++ List(p.value))))

    val (unDeSerIntIndexes, unDeSerBinIndexes) = indexes.partition(tpl ⇒ isIntIndex(tpl._1))

    val intIndexes: Map[String, List[Int]] = unDeSerIntIndexes.map(tpl ⇒ indexName(tpl._1) -> tpl._2.filter(x ⇒ x.isDefined).map(_.get.toStringUtf8.toInt))
    val binIndexes: Map[String, List[String]] = unDeSerBinIndexes.map(tpl ⇒ indexName(tpl._1) -> tpl._2.filter(x ⇒ x.isDefined).map(_.get.toStringUtf8))

    RaikuRWObject(bucket, key, c.value.toByteArray, c.contentType.map(_.toStringUtf8).getOrElse("text/plain"), vClock,
      c.vtag.map(_.toByteArray).map(VTag(_)), c.lastMod, c.lastModUsecs, userMeta, intIndexes, binIndexes, c.deleted)
  }

  def pbPutRespToRWObjects(key: String, bucket: String, gr: com.basho.riak.protobuf.RpbPutResp): List[RaikuRWObject] = {
    val content: Vector[RpbContent] = gr.content
    content.map { c ⇒
      pbContentToRWObject(key, bucket, c, gr.vclock.map(_.toByteArray).map(VClock(_)))
    }.toList
  }

  def pbGetRespToRWObjects(key: String, bucket: String, gr: com.basho.riak.protobuf.RpbGetResp): List[RaikuRWObject] = {
    val content: Vector[RpbContent] = gr.content
    content.map { c ⇒
      pbContentToRWObject(key, bucket, c, gr.vclock.map(_.toByteArray).map(VClock(_)))
    }.toList
  }
}

