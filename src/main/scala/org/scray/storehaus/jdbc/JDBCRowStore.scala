package org.scray.storehaus.jdbc

import com.twitter.storehaus.Store
import com.twitter.util.Future
import scalikejdbc._
import com.twitter.util.Duration
import org.scray.storehaus.jdbc.types.StorehausJDBCTypes
import java.sql.Connection
import org.scray.storehaus.jdbc.types.Row
import scala.collection.mutable.ListBuffer
import org.scray.storehaus.jdbc.types.JDBCToScalaTypes
import scala.collection.immutable.TreeMap
import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.IterableStore
import com.twitter.storehaus.ReadableStore
import com.twitter.concurrent.Spool
import org.scray.storehaus.jdbc.types.JDBCToScalaTypesConfig

/**
 * Simple store implementation for JDBC stores.
 * Uses org.scray.storehaus.jdbc.types.Row as values.
 * 
 * WARNING: The table parameter has been manually cleaned 
 * and therefore not safe against SQL injection.
 */
class JDBCRowStore[K: TypeBinder](
    override val pool: ConnectionPool,
    override val table: String,
    keyColumnName: String = "key",
    override val poolSize: Int = 10,
    override val shutdownTimeout: Duration = Duration.fromSeconds(120),
    typeMapping: Option[JDBCToScalaTypesConfig] = None, // None means default type mapping
    override val fetchSize: Int = 500) extends 
    AbstractJDBCStore[K, Row](pool, table, poolSize, shutdownTimeout) with 
    Store[K, Row] with
    IterableStore[K, Row] with
    QueryableStore[SQLSyntax, (K, Row)] {

  val cleanedKeyColumnName = filterName(keyColumnName)
  
  val sorting: Ordering[(String, String)] = new Ordering[(String, String)] {
    val strOrdering = implicitly[Ordering[String]]
    def compare(x: (String, String), y: (String, String)): Int = strOrdering.compare(x._1, y._1)
  } 

  /**
   * map resultset into a row
   */
  lazy val rowMapping: WrappedResultSet => Row = { rs =>
    val cols = columns.map { col =>
      typeMapping.map { tm =>
        if(tm.enforceUsingNoMapping) {
          col._1 -> rs.any(col._1)
        } else {
          col._1 -> rs.any(col._1, tm.mapping.getOrElse(JDBCToScalaTypes.typeMapping))
        }
      }.getOrElse {
        col._1 -> rs.any(col._1, JDBCToScalaTypes.typeMapping)
      }
    }.toMap
    Row(cols)
  }

  /**
   * creates an sql insert statement for a new row
   */
  lazy val sqlInsertStatement: String =
    s"INSERT INTO $cleanedTableName (" +  
      cleanedColumns.map(_._1).mkString(", ") +
      ") VALUES (" +
      cleanedColumns.map(_ => " ? ").mkString(", ") +
      ")"
  
  /**
   * creates an sql update statement for a row with the given key
   */
  lazy val sqlUpdateStatement: String =
    s"UPDATE $cleanedTableName SET " +  
      cleanedColumns.map(_._1 + " = ? ").mkString(", ") +
      " WHERE " + 
      cleanedKeyColumnName + 
      " = ?"

  /**
   * delete statement for a row with the given key
   */
  lazy val sqlDeleteStatement: String =
    s"DELETE FROM $cleanedTableName WHERE " + 
      cleanedKeyColumnName + 
      " = ?"
  
  override def put(kv: (K, Option[Row])): Future[Unit] = futurePool {
    logger.debug(s"Writing value with key ${kv._1}")
    def execute(db: DBSession, stmtStr: String, values: Option[Row], additionalValues: List[(String, _)] = List()): Int = {
      val stmt = db.connection.prepareStatement(stmtStr)
      var index = 0
      values.map { vals =>
        cleanedColumns.map { colname =>
          vals.columns.get(colname._1).map { colval =>
            index += 1
            stmt.setObject(index, colval)
          }
        }
      }
      additionalValues.foreach { 
        index += 1
        value => stmt.setObject(index, value._2)
      }
      stmt.executeUpdate()
    }
    DB(pool.borrow()) localTx { db =>
      get(kv._1, db).map { row =>
        kv._2 match {
          case Some(row) => execute(db, sqlUpdateStatement, kv._2, List((cleanedKeyColumnName, kv._1)))
          case None => execute(db, sqlDeleteStatement, None, List((cleanedKeyColumnName, kv._1)))
        }
      }.getOrElse {
        execute(db, sqlInsertStatement, kv._2)
      }
    }
  }

  private def get(k: K, session: DBSession): Option[Row] = {
    implicit val sess = session
    session.fetchSize(fetchSize)
    sql"SELECT * FROM ${createdTableName} WHERE key = ${k}".map(rs => Row(rs, rowMapping)).single.apply()
  }
  
  override def get(k: K): Future[Option[Row]] = futurePool {
    using(DB(pool.borrow())) { db =>
      db.readOnly { implicit session => get(k, session) }
    }
  }
  
  override def resultFromWrappedResultSet(row: WrappedResultSet): (K, Row) = (row.get(keyColumnName), rowMapping(row))
  
}
