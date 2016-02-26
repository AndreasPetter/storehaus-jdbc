package org.scray.storehaus.jdbc.types

import java.math.{BigInteger, BigDecimal => JBigDecimal}
import java.util.{ Map => JMap }
import java.sql.{Array => JArray, Blob, Clob, Date => JDate, NClob, Ref, RowId, SQLXML, Time, Timestamp, Struct}
import java.net.URL
import scala.collection.convert.decorateAsJava.mapAsJavaMapConverter
import scalikejdbc.WrappedResultSet

object JDBCToScalaTypes {
  
  /**
   * The default mapping of types used in this library.
   * However, some stores, such as H2 are not able to be used with type mappings.
   * Use JDBCToScalaTypesConfig.enforceUsingNoMapping = true in these cases.
   */
  lazy val typeMapping : Map[String, Class[_]] = Map[String, Class[_]] (
    "BIT" -> classOf[Boolean],
    "TINYINT" -> classOf[Byte],
    "SMALLINT" -> classOf[Short],
    "INTEGER" -> classOf[Int],
    "BIGINT" -> classOf[BigInteger],
    "FLOAT" -> classOf[Float],
    "REAL" -> classOf[Float],
    "DOUBLE" -> classOf[Double],
    "NUMERIC" -> classOf[JBigDecimal],
    "DECIMAL" -> classOf[JBigDecimal],
    "CHAR" -> classOf[String],
    "NCHAR" -> classOf[String],
    "VARCHAR" -> classOf[String],
    "NVARCHAR" -> classOf[String],
    "LONGVARCHAR" -> classOf[String],
    "LONGNVARCHAR" -> classOf[String],
    "DATE" -> classOf[JDate],
    "TIME" -> classOf[Time],
    "TIMESTAMP" -> classOf[Timestamp],
    "BINARY" -> classOf[Array[Byte]],
    "VARBINARY" -> classOf[Array[Byte]],
    "LONGVARBINARY" -> classOf[Array[Byte]],
    "STRUCT" -> classOf[Struct],
    "ARRAY" -> classOf[JArray],
    "BLOB" -> classOf[Blob],
    "REF" -> classOf[Ref],
    "CLOB" -> classOf[Clob],
    "BOOLEAN" -> classOf[Boolean],
    "ROWID" -> classOf[RowId],
    "DATALINK" -> classOf[URL],
    "SQLXML" -> classOf[SQLXML],
    "NCLOB" -> classOf[NClob]
  )
}

/**
 * used to define the mapping features of a store
 */
case class JDBCToScalaTypesConfig(mapping: Option[Map[String, Class[_]]], enforceUsingNoMapping: Boolean = false)
