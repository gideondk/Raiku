package nl.gideondk.raiku

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

  def write(bucket: String, bucketType: Option[String], o: T): RaikuValue[T]
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

      RaikuRawValue(o.bucket, o.key, o.bucketType, rwObj.map(_.contentType), None, None, rwObj.map(_.data), o.meta)
    }

    def readRaw(o: RaikuRawValue) =
      RaikuValue(o.bucket, o.key, o.bucketType, o.value.map(x ⇒ readData(RaikuRWValue(o.key, x, o.contentType.getOrElse("text/plain")))), o.meta)

    def write(bucket: String, bucketType: Option[String], o: T) = {
      val meta = RaikuMeta(extractIndexes(o))
      RaikuValue(bucket, writeData(o).key, bucketType, Some(o), Some(meta))
    }
  }
}