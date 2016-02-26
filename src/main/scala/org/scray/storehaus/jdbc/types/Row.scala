package org.scray.storehaus.jdbc.types

import scalikejdbc.WrappedResultSet

/**
 * reflects a Row in the RowStore
 */
case class Row(columns: Map[String, _])

object Row {
  
  /**
   * transform ScalikeJDBC's WrappedResultSet into a Row
   */
  def apply(rs: WrappedResultSet, mapping: WrappedResultSet => Row): Row = mapping(rs)
}