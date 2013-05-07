package nl.gideondk.raiku

/* 
	RaikuMeta: used for the storage of Riak meta information
*/

case class RaikuMeta(indexes: RaikuIndexes, vTag: Option[VTag] = None, lastModified: Option[Int] = None, lastModifiedMicros: Option[Int] = None,
                     userMeta: Map[String, Option[Array[Byte]]] = Map[String, Option[Array[Byte]]](), deleted: Option[Boolean] = None)

/* 
	RaikuRawValue: the raw Riak value, combined with meta information
*/

case class RaikuRawValue(bucket: String, key: String, contentType: Option[String], charset: Option[String], contentEncoding: Option[String],
                         value: Option[Array[Byte]], meta: Option[RaikuMeta])

/* 
	RaikuValue: a converted (typed) Riak value, combined with meta information
*/

case class RaikuValue[T](bucket: String, key: String, value: Option[T], meta: Option[RaikuMeta])

/* 
	RaikuRWValue: typed used for (de)serialization of Riak values
*/

case class RaikuRWValue(key: String, data: Array[Byte], contentType: String)

/* 
	RaikuIndexes: type containing both binary as integer indexes used within metas
*/

case class RaikuIndexes(binary: Map[String, Set[String]], integer: Map[String, Set[Int]])

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

case class VClock(v: Array[Byte])

case class VTag(v: Array[Byte])

case class UnresolvedSiblingsConflict extends Exception