package spark.jobserver

import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * wrapper for named objects of type DataFrame
 */
case class NamedDataFrame(df: DataFrame, forceComputation: Boolean,
                          storageLevel: StorageLevel) extends NamedObject

/**
 * implementation of a NamedObjectPersister for DataFrame objects
 *
 */
class DataFramePersister extends NamedObjectPersister[NamedDataFrame] {
  override def persist(namedObj: NamedDataFrame, name: String) {
    namedObj match {
      case NamedDataFrame(df, forceComputation, storageLevel) =>
        require(!forceComputation || storageLevel != StorageLevel.NONE,
          "forceComputation implies storageLevel != NONE")
        //these are not supported by DataFrame:
        //df.setName(name)
        //df.getStorageLevel match
        df.persist(storageLevel)
        // perform some action to force computation
        if (forceComputation) df.count()
    }
  }

  override def unpersist(namedObj: NamedDataFrame) {
    namedObj match {
      case NamedDataFrame(df, _, _) =>
        df.unpersist(blocking = false)
    }
  }
}