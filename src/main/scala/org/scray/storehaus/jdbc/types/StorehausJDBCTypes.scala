package org.scray.storehaus.jdbc.types

/**
 * to 
 */
trait StorehausJDBCTypes[T] {
  val DBTypeName: String
}

object simpleTypes {
  
  implicit object StringStorehausJDBCType extends StorehausJDBCTypes[String] {
    val DBTypeName = "VARCHAR"
  }
  
}