package org.scray.storehaus.jdbc

import scalikejdbc.interpolation.SQLSyntax
import shapeless._
import scala.collection.mutable.ArrayBuffer
import scalikejdbc._
import scala.annotation.tailrec
import org.scray.storehaus.jdbc.types.Row
import shapeless.syntax.std.traversable._
import shapeless.ops.hlist._
import shapeless.ops.traversable.FromTraversable

/**
 * Translate HList based Keys or Values
 */
object HandleComposites {
  
  /**
   * the default SQL builder of scalikeJDBC is not
   * a stateful builder
   */
  trait StatefulSQLBuilder[T <: StatefulSQLBuilder[T]] {
    def build: SQLSyntax
    def add(column: SQLSyntax, value: SQLSyntax): T
  }
  
  /**
   * a stateful insert statement
   */
  object StatefulInsert {
    def into(table: SQLSyntax): StatefulInsert =
      new StatefulInsert(sqls"INSERT INTO ${table} ")
  }
  
  private[jdbc] class StatefulInsert(sqlsyntax: SQLSyntax) extends StatefulSQLBuilder[StatefulInsert] {
    val columns: ArrayBuffer[SQLSyntax] = new ArrayBuffer[SQLSyntax]
    val values: ArrayBuffer[SQLSyntax] = new ArrayBuffer[SQLSyntax]
    
    override def build: SQLSyntax = {
      @tailrec def entrySyntax(pos: Int, sql: SQLSyntax, arr: ArrayBuffer[SQLSyntax]): SQLSyntax = {
        if(pos < columns.length - 1) {
          entrySyntax(pos + 1, sql.append(sqls"${arr(pos)},"), arr)
        } else {
          sql.append(sqls"${arr(pos)}")
        }
      }
      sqlsyntax.append(sqls" (${entrySyntax(0, sqls"", columns)}) VALUES (${entrySyntax(0, sqls"", values)})")
    }
    
    override def add(column: SQLSyntax, value: SQLSyntax): StatefulInsert = this.value(column, value)
    
    def value(column: SQLSyntax, value: SQLSyntax): StatefulInsert = {
      columns += column
      values += value
      this
    }
  }
  
  /**
   * factored out where like syntax handling
   */
  private[jdbc] abstract class StatefulWhereLike[T <: StatefulWhereLike[T]](sqlsyntax: SQLSyntax) extends StatefulSQLBuilder[T] { self: T =>
    val columns: ArrayBuffer[SQLSyntax] = new ArrayBuffer[SQLSyntax]
    val values: ArrayBuffer[SQLSyntax] = new ArrayBuffer[SQLSyntax]
    
    val seperator: String
    lazy val sep = SQLSyntax.createUnsafely(seperator) 
    
    @tailrec final def entrySyntax(pos: Int, sql: SQLSyntax, cols: ArrayBuffer[SQLSyntax], vals: ArrayBuffer[SQLSyntax], colsep: SQLSyntax = sep): SQLSyntax = {
      if(pos < cols.length - 1) {
        entrySyntax(pos + 1, sql.append(sqls"${cols(pos)} = ${vals(pos)} ${colsep} "), cols, vals)
      } else {
        sql.append(sqls"${cols(pos)} = ${vals(pos)}")
      }
    }

    override def add(column: SQLSyntax, value: SQLSyntax): T = self.where(column, value)
    
    def where(column: SQLSyntax, value: SQLSyntax): T = {
      columns += column
      values += value
      self
    }
  }
  
  
  /**
   * a stateful update statement
   */
  object StatefulUpdate {
    def update(table: SQLSyntax): StatefulUpdate =
      new StatefulUpdate(sqls"UPDATE ${table} SET ")
  }
  
  private[jdbc] class StatefulUpdate(sqlsyntax: SQLSyntax) extends StatefulWhereLike[StatefulUpdate](sqlsyntax) {
    val setcolumns: ArrayBuffer[SQLSyntax] = new ArrayBuffer[SQLSyntax]
    val setvalues: ArrayBuffer[SQLSyntax] = new ArrayBuffer[SQLSyntax]
    
    override val seperator: String = ","
    val wheresep = SQLSyntax.createUnsafely("AND")
    
    override def build: SQLSyntax = {
      sqlsyntax.append(sqls" ${entrySyntax(0, sqls"", setcolumns, setvalues)} WHERE ${entrySyntax(0, sqls"", columns, values, wheresep)}")
    }
    
    def set(column: SQLSyntax, value: SQLSyntax): StatefulUpdate = {
      setcolumns += column
      setvalues += value
      this
    }
  }
  
  /**
   * a stateful delete statement
   */
  object StatefulDelete {
    def from(table: SQLSyntax): StatefulDelete =
      new StatefulDelete(sqls"DELETE FROM ${table} WHERE ")
  }
  
  private[jdbc] class StatefulDelete(sqlsyntax: SQLSyntax) extends StatefulWhereLike[StatefulDelete](sqlsyntax) {
    override val seperator: String = "AND"
    override def build: SQLSyntax = {
      sqlsyntax.append(sqls" ${entrySyntax(0, sqls"", columns, values)}")
    }
  }
  
  /**
   * a stateful select statement
   * This is not meant to be complete, but just to fit in here!
   */
  object StatefulSelect {
    def from(table: SQLSyntax): StatefulSelect =
      new StatefulSelect(sqls"SELECT * FROM ${table} WHERE ")
  }
  
  private[jdbc] class StatefulSelect(sqlsyntax: SQLSyntax) extends StatefulWhereLike[StatefulSelect](sqlsyntax) {
    override val seperator: String = "AND"
    override def build: SQLSyntax = {
      sqls"${sqlsyntax} ${entrySyntax(0, sqls"", columns, values)}"
      // sqlsyntax.append(sqls" ${entrySyntax(0, sqls"", columns, values)}")
    }
  }
  
  /**
   * helper trait for declaring the HList recursive function 
   * to append keys on an ArrayBuffer in a type safe way
   */
  trait Append2JDBCComposite[T <: StatefulSQLBuilder[T], K <: HList, Q <: HList] {
    def apply(sql: T, k: K, s: List[String], ser: Q): T
  }

  /**
   * helper implicits for the recursion itself
   */
  object Append2JDBCComposite {
    implicit def hnilAppend2Composite[T <: StatefulSQLBuilder[T]]: Append2JDBCComposite[T, HNil, HNil] = 
      new Append2JDBCComposite[T, HNil, HNil] {
        override def apply(sql: T, k: HNil, s: List[String], ser: HNil): T = sql
      }
    implicit def hlistAppend2Composite[T <: StatefulSQLBuilder[T], M, K <: HList, Q <: HList](
      implicit a2c: Append2JDBCComposite[T, K, Q]): Append2JDBCComposite[T, M :: K, TypeBinder[M] :: Q] =
        new Append2JDBCComposite[T, M :: K, TypeBinder[M] :: Q] {
    	    override def apply(sql: T, k: M :: K, s: List[String], ser: TypeBinder[M] :: Q): T = {
   	        implicit val typeBinder = ser.head
    	      val column = SQLSyntax.createUnsafely(s.head)
    	      val newSQL = sql.add(sqls"${column}", sqls"${k.head}")
    	      a2c(newSQL, k.tail, s.tail, ser.tail)
    	    }
        }
  }

  /**
   * recursive function callee implicits for inserts and updates
   */
  implicit def append2composite[T <: StatefulSQLBuilder[T], K <: HList, S <: List[String], Q <: HList](sql: T, k: K, s: List[String], ser: Q)
  	(implicit a2c: Append2JDBCComposite[T, K, Q]) = a2c(sql, k, s, ser)
    
  /**
   * extracts an HList key from a Row
   */
  def transformRowToKey[K <: HList](row: Row, keycolumns: List[String])(implicit ft: FromTraversable[K]): Option[K] = 
    keycolumns.map(col => row.columns.get(col).get).toHList[K]
}