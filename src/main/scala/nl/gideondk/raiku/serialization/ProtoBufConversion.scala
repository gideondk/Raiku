package nl.gideondk.raiku.serialization

import com.google.protobuf.MessageLite
import com.google.protobuf.CodedOutputStream
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import akka.util.ByteString

import scalaz._
import Scalaz._

import com.google.protobuf.{ ByteString ⇒ ProtoBufByteString }
import com.basho.riak.protobuf._
import nl.gideondk.raiku.commands._
import nl.gideondk.raiku._

trait ProtoBufConversion {
  implicit def stringToByteString(s: String): ProtoBufByteString = ProtoBufByteString.copyFromUtf8(s)

  implicit def byteStringToString(s: ProtoBufByteString): String = s.toStringUtf8

  implicit def byteStringToByteArray(s: ProtoBufByteString): Array[Byte] = s.toByteArray

  implicit def byteArrayToByteString(s: Array[Byte]): ProtoBufByteString = ProtoBufByteString.copyFrom(s)

  implicit def messageToByteArray[T <: MessageLite with MessageLite.Builder](m: com.basho.riak.protobuf.Message[T]) = {
    val os = new ByteArrayOutputStream()
    val cos = CodedOutputStream.newInstance(os)
    m.writeTo(cos)
    cos.flush()
    os.close()
    os.toByteArray
  }

  implicit def messageToByteString[T <: MessageLite with MessageLite.Builder](m: com.basho.riak.protobuf.Message[T]) = {
    val os = new ByteArrayOutputStream()
    val cos = CodedOutputStream.newInstance(os)
    m.writeTo(cos)
    cos.flush()
    os.close()
    ByteString(os.toByteArray)
  }

  def request(messageType: RiakMessageType): ByteString = {
    val bsb = ByteString.newBuilder
    val cos = new DataOutputStream(bsb.asOutputStream)
    cos.writeInt(1)
    cos.write(RiakMessageType.messageTypeToInt(messageType))
    cos.flush()
    cos.close()
    bsb.result()
  }

  def request(messageType: RiakMessageType, message: Array[Byte]): ByteString = {
    val bsb = ByteString.newBuilder
    val cos = new DataOutputStream(bsb.asOutputStream)

    cos.writeInt(message.length + 1)
    cos.write(RiakMessageType.messageTypeToInt(messageType))
    cos.write(message)
    cos.flush()
    cos.close()
    bsb.result()
  }

  def rawValueToRpbContent(rawValue: RaikuRawValue) = {
    // TODO: implement links

    val indexes = rawValue.meta.map { meta ⇒
      meta.indexes.binary.map(tpl ⇒ tpl._2.map(x ⇒ RpbPair(stringToByteString(tpl._1+"_bin"), stringToByteString(x).some))).flatten ++
        meta.indexes.integer.map(tpl ⇒ tpl._2.map(x ⇒ RpbPair(stringToByteString(tpl._1+"_int"), stringToByteString(x.toString).some))).flatten

    }

    RpbContent(rawValue.value.get, rawValue.contentType.map(stringToByteString), rawValue.charset.map(stringToByteString),
      rawValue.contentEncoding.map(stringToByteString), rawValue.meta.map(x ⇒ x.vTag.map(x ⇒ byteArrayToByteString(x.v))).flatten, Vector[RpbLink](),
      ~rawValue.meta.map(_.lastModified), ~rawValue.meta.map(_.lastModifiedMicros), ~rawValue.meta.map(_.userMeta.map(tpl ⇒ RpbPair(tpl._1, tpl._2.map(byteArrayToByteString(_)))).toVector),
      ~indexes.map(_.toVector), rawValue.meta.map(_.deleted).flatten)
  }

  def pbContentToRawValue(key: String, bucket: String, c: RpbContent): RaikuRawValue = {
      def isBinIndex(idx: String) = idx.substring(idx.length - 4) == "_bin"
      def isIntIndex(idx: String) = idx.substring(idx.length - 4) == "_int"
      def indexName(idx: String) = idx.substring(0, idx.length - 4)

    val userMeta: Map[String, Option[Array[Byte]]] = c.usermeta.map(p ⇒ p.key.toStringUtf8 -> p.value.map(_.toByteArray)).toMap

    val indexes: Map[String, List[Option[com.google.protobuf.ByteString]]] =
      c.indexes.foldLeft(Map[String, List[Option[com.google.protobuf.ByteString]]]())((m, p) ⇒ m + (p.key.toStringUtf8 -> (~m.get(p.key.toStringUtf8) ++ List(p.value))))

    val (unDeSerIntIndexes, unDeSerBinIndexes) = indexes.partition(tpl ⇒ isIntIndex(tpl._1))

    val intIndexes: Map[String, Set[Int]] = unDeSerIntIndexes.map(tpl ⇒ indexName(tpl._1) -> tpl._2.filter(x ⇒ x.isDefined).map(_.get.toStringUtf8.toInt).toSet)
    val binIndexes: Map[String, Set[String]] = unDeSerBinIndexes.map(tpl ⇒ indexName(tpl._1) -> tpl._2.filter(x ⇒ x.isDefined).map(_.get.toStringUtf8).toSet)

    val ba = c.value.toByteArray
    val value = if (ba.length == 0) None else Some(ba.toByteArray)

    val meta = RaikuMeta(RaikuIndexes(binIndexes, intIndexes), c.vtag.map(x ⇒ VTag(x.toByteArray)), c.lastMod, c.lastModUsecs, userMeta, c.deleted)
    RaikuRawValue(bucket, key, c.contentType.map(_.toStringUtf8), c.charset.map(_.toStringUtf8), c.contentEncoding.map(_.toStringUtf8), value, Some(meta))
  }

  def pbPutRespToRawValues(key: String, bucket: String, gr: com.basho.riak.protobuf.RpbPutResp): Set[RaikuRawValue] = {
    val content: Vector[RpbContent] = gr.content
    content.map(pbContentToRawValue(key, bucket, _)).toSet
  }

  def pbGetRespToRawValues(key: String, bucket: String, gr: com.basho.riak.protobuf.RpbGetResp): Set[RaikuRawValue] = {
    val content: Vector[RpbContent] = gr.content
    content.map(pbContentToRawValue(key, bucket, _)).toSet
  }
}

