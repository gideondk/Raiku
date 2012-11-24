// Generated by ScalaBuff, the Scala Protocol Buffers compiler. DO NOT EDIT!
// source: riak.proto

package com.basho.riak.protobuf

final case class RpbErrorResp (
	`errmsg`: com.google.protobuf.ByteString = com.google.protobuf.ByteString.EMPTY,
	`errcode`: Int = 0
) extends com.google.protobuf.GeneratedMessageLite
	with Message[RpbErrorResp] {


	def clearErrmsg = copy(`errmsg` = com.google.protobuf.ByteString.EMPTY)
	def clearErrcode = copy(`errcode` = 0)

	def writeTo(output: com.google.protobuf.CodedOutputStream) {
		output.writeBytes(1, `errmsg`)
		output.writeUInt32(2, `errcode`)
	}

	lazy val getSerializedSize = {
		import com.google.protobuf.CodedOutputStream._
		var size = 0
		size += computeBytesSize(1, `errmsg`)
		size += computeUInt32Size(2, `errcode`)

		size
	}

	def mergeFrom(in: com.google.protobuf.CodedInputStream, extensionRegistry: com.google.protobuf.ExtensionRegistryLite): RpbErrorResp = {
		import com.google.protobuf.ExtensionRegistryLite.{getEmptyRegistry => _emptyRegistry}
		var __errmsg: com.google.protobuf.ByteString = com.google.protobuf.ByteString.EMPTY
		var __errcode: Int = 0

		def __newMerged = RpbErrorResp(
			__errmsg,
			__errcode
		)
		while (true) in.readTag match {
			case 0 => return __newMerged
			case 10 => __errmsg = in.readBytes()
			case 16 => __errcode = in.readUInt32()
			case default => if (!in.skipField(default)) return __newMerged
		}
		null
	}

	def mergeFrom(m: RpbErrorResp) = {
		RpbErrorResp(
			m.`errmsg`,
			m.`errcode`
		)
	}

	def getDefaultInstanceForType = RpbErrorResp.defaultInstance
	def clear = getDefaultInstanceForType
	def isInitialized = true
	def build = this
	def buildPartial = this
	def newBuilderForType = this
	def toBuilder = this
}

object RpbErrorResp {
	@reflect.BeanProperty val defaultInstance = new RpbErrorResp()

	val ERRMSG_FIELD_NUMBER = 1
	val ERRCODE_FIELD_NUMBER = 2

}
final case class RpbGetServerInfoResp (
	`node`: Option[com.google.protobuf.ByteString] = None,
	`serverVersion`: Option[com.google.protobuf.ByteString] = None
) extends com.google.protobuf.GeneratedMessageLite
	with Message[RpbGetServerInfoResp] {

	def setNode(_f: com.google.protobuf.ByteString) = copy(`node` = _f)
	def setServerVersion(_f: com.google.protobuf.ByteString) = copy(`serverVersion` = _f)

	def clearNode = copy(`node` = None)
	def clearServerVersion = copy(`serverVersion` = None)

	def writeTo(output: com.google.protobuf.CodedOutputStream) {
		if (`node`.isDefined) output.writeBytes(1, `node`.get)
		if (`serverVersion`.isDefined) output.writeBytes(2, `serverVersion`.get)
	}

	lazy val getSerializedSize = {
		import com.google.protobuf.CodedOutputStream._
		var size = 0
		if (`node`.isDefined) size += computeBytesSize(1, `node`.get)
		if (`serverVersion`.isDefined) size += computeBytesSize(2, `serverVersion`.get)

		size
	}

	def mergeFrom(in: com.google.protobuf.CodedInputStream, extensionRegistry: com.google.protobuf.ExtensionRegistryLite): RpbGetServerInfoResp = {
		import com.google.protobuf.ExtensionRegistryLite.{getEmptyRegistry => _emptyRegistry}
		var __node: Option[com.google.protobuf.ByteString] = `node`
		var __serverVersion: Option[com.google.protobuf.ByteString] = `serverVersion`

		def __newMerged = RpbGetServerInfoResp(
			__node,
			__serverVersion
		)
		while (true) in.readTag match {
			case 0 => return __newMerged
			case 10 => __node = in.readBytes()
			case 18 => __serverVersion = in.readBytes()
			case default => if (!in.skipField(default)) return __newMerged
		}
		null
	}

	def mergeFrom(m: RpbGetServerInfoResp) = {
		RpbGetServerInfoResp(
			m.`node`.orElse(`node`),
			m.`serverVersion`.orElse(`serverVersion`)
		)
	}

	def getDefaultInstanceForType = RpbGetServerInfoResp.defaultInstance
	def clear = getDefaultInstanceForType
	def isInitialized = true
	def build = this
	def buildPartial = this
	def newBuilderForType = this
	def toBuilder = this
}

object RpbGetServerInfoResp {
	@reflect.BeanProperty val defaultInstance = new RpbGetServerInfoResp()

	val NODE_FIELD_NUMBER = 1
	val SERVER_VERSION_FIELD_NUMBER = 2

}
final case class RpbPair (
	`key`: com.google.protobuf.ByteString = com.google.protobuf.ByteString.EMPTY,
	`value`: Option[com.google.protobuf.ByteString] = None
) extends com.google.protobuf.GeneratedMessageLite
	with Message[RpbPair] {

	def setValue(_f: com.google.protobuf.ByteString) = copy(`value` = _f)

	def clearKey = copy(`key` = com.google.protobuf.ByteString.EMPTY)
	def clearValue = copy(`value` = None)

	def writeTo(output: com.google.protobuf.CodedOutputStream) {
		output.writeBytes(1, `key`)
		if (`value`.isDefined) output.writeBytes(2, `value`.get)
	}

	lazy val getSerializedSize = {
		import com.google.protobuf.CodedOutputStream._
		var size = 0
		size += computeBytesSize(1, `key`)
		if (`value`.isDefined) size += computeBytesSize(2, `value`.get)

		size
	}

	def mergeFrom(in: com.google.protobuf.CodedInputStream, extensionRegistry: com.google.protobuf.ExtensionRegistryLite): RpbPair = {
		import com.google.protobuf.ExtensionRegistryLite.{getEmptyRegistry => _emptyRegistry}
		var __key: com.google.protobuf.ByteString = com.google.protobuf.ByteString.EMPTY
		var __value: Option[com.google.protobuf.ByteString] = `value`

		def __newMerged = RpbPair(
			__key,
			__value
		)
		while (true) in.readTag match {
			case 0 => return __newMerged
			case 10 => __key = in.readBytes()
			case 18 => __value = in.readBytes()
			case default => if (!in.skipField(default)) return __newMerged
		}
		null
	}

	def mergeFrom(m: RpbPair) = {
		RpbPair(
			m.`key`,
			m.`value`.orElse(`value`)
		)
	}

	def getDefaultInstanceForType = RpbPair.defaultInstance
	def clear = getDefaultInstanceForType
	def isInitialized = true
	def build = this
	def buildPartial = this
	def newBuilderForType = this
	def toBuilder = this
}

object RpbPair {
	@reflect.BeanProperty val defaultInstance = new RpbPair()

	val KEY_FIELD_NUMBER = 1
	val VALUE_FIELD_NUMBER = 2

}

object RiakPB {
	def registerAllExtensions(registry: com.google.protobuf.ExtensionRegistryLite) {
	}

}
