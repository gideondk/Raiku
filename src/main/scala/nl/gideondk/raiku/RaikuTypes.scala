package nl.gideondk.raiku

import scala.util.Try
import scalaz._
import Scalaz._

case class VClock(v: Array[Byte])

case class VTag(v: Array[Byte])

case class RaikuMeta(indexes: RaikuIndexes, vTag: Option[VTag] = None, lastModified: Option[Int] = None, lastModifiedMicros: Option[Int] = None,
                     userMeta: Map[String, Option[Array[Byte]]] = Map[String, Option[Array[Byte]]](), deleted: Option[Boolean] = None)

case class RaikuRawValue(bucket: String, key: String, contentType: Option[String], charset: Option[String], contentEncoding: Option[String],
                         value: Option[Array[Byte]], meta: Option[RaikuMeta])

case class RaikuValue[T](bucket: String, key: String, value: Option[T], meta: Option[RaikuMeta])

case class RaikuRWValue(key: String, data: Array[Byte], contentType: String)

case class RaikuIndexes(binary: Map[String, Set[String]], integer: Map[String, Set[Int]])

trait RaikuIndexReader[T] {
  def extractIndexes(o: T): RaikuIndexes
}

trait RaikuContentConverter[T] {
  // Unsafe method, could throw exceptions which should be handled within Tasks 
  def readData(o: RaikuRWValue): T // Key + Data + ContentType

  def writeData(o: T): RaikuRWValue
}

trait RaikuValueConverter[T] extends RaikuIndexReader[T] with RaikuContentConverter[T] {
  def readRaw(o: RaikuRawValue): RaikuValue[T]

  def writeToRaw(o: RaikuValue[T]): RaikuRawValue

  def write(bucket: String, o: T): RaikuValue[T]
}

object RaikuConverter {
  def newConverter[T](reader: RaikuRWValue ⇒ T, writer: T ⇒ RaikuRWValue,
                      binIndexes: T ⇒ Map[String, Set[String]] = (o: T) ⇒ Map[String, Set[String]](),
                      intIndexes: T ⇒ Map[String, Set[Int]] = (o: T) ⇒ Map[String, Set[Int]]()) = new RaikuValueConverter[T] {
    def extractIndexes(o: T) = RaikuIndexes(binIndexes(o), intIndexes(o))

    def readData(o: RaikuRWValue) = reader(o)

    def writeData(o: T) = writer(o)

    def writeToRaw(o: RaikuValue[T]) = {
      val rwObj = o.value.map(writeData(_))

      RaikuRawValue(o.bucket, o.key, rwObj.map(_.contentType), None, None, rwObj.map(_.data), o.meta)
    }

    def readRaw(o: RaikuRawValue) =
      RaikuValue(o.bucket, o.key, o.value.map(x ⇒ readData(RaikuRWValue(o.key, x, o.contentType.getOrElse("text/plain")))), o.meta)

    def write(bucket: String, o: T) = {
      val meta = RaikuMeta(extractIndexes(o))
      RaikuValue(bucket, writeData(o).key, Some(o), Some(meta))
    }
  }
}

case class UnresolvedSiblingsConflict extends Exception

/* Resolvers */
trait RaikuResolver[T] {
  def apply(siblings: Set[RaikuValue[T]]): Option[RaikuValue[T]]
}

object RaikuResolver {
  def apply[T](f: Set[RaikuValue[T]] ⇒ Option[RaikuValue[T]]) = new RaikuResolver[T] {
    def apply(siblings: Set[RaikuValue[T]]) = f(siblings)
  }

  def throwConflicts[T] = apply((a: Set[RaikuValue[T]]) ⇒ throw new UnresolvedSiblingsConflict())
}

/* Mutators */
trait RaikuMutator[T] {
  def apply(storedObj: Option[RaikuValue[T]], newObj: RaikuValue[T]): RaikuValue[T]
}

object RaikuMutator {
  def apply[T](f: (Option[RaikuValue[T]], RaikuValue[T]) ⇒ RaikuValue[T]) = new RaikuMutator[T] {
    def apply(storedObj: Option[RaikuValue[T]], newObj: RaikuValue[T]) = f(storedObj, newObj)
  }

  def clobber[T] = apply((a: Option[RaikuValue[T]], b: RaikuValue[T]) ⇒ b)
}
