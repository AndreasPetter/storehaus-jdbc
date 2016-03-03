package org.scray.storehaus.jdbc

import java.sql.Connection
import org.scray.storehaus.jdbc.HandleComposites.StatefulSelect
import org.scray.storehaus.jdbc.types.JDBCToScalaTypes
import org.scray.storehaus.jdbc.types.JDBCToScalaTypesConfig
import org.scray.storehaus.jdbc.types.Row
import com.twitter.storehaus.IterableStore
import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.Store
import com.twitter.util.Duration
import com.twitter.util.Future
import HandleComposites._
import scalikejdbc._
import shapeless._
import shapeless.ops.traversable.FromTraversable

abstract class AbstractJDBCRowStore[K](
    override val pool: ConnectionPool,
    override val table: String,
    override val poolSize: Int = 10,
    override val shutdownTimeout: Duration = Duration.fromSeconds(120),
    override val fetchSize: Int = 500,
    val typeMapping: Option[JDBCToScalaTypesConfig] = None) extends // None means default type mapping) extends 
      AbstractJDBCStore[K, Row](pool, table, poolSize, shutdownTimeout, fetchSize) with
      Store[K, Row] with
      IterableStore[K, Row] with
      QueryableStore[SQLSyntax, (K, Row)] {

  protected def get(k: K, session: DBSession): Option[Row]
  
  override def get(k: K): Future[Option[Row]] = futurePool {
    using(DB(pool.borrow())) { db =>
      db.readOnly { implicit session =>
        session.fetchSize(fetchSize)
        get(k, session) 
      }
    }
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

}


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
    override val typeMapping: Option[JDBCToScalaTypesConfig] = None, // None means default type mapping
    override val fetchSize: Int = 500) extends 
    AbstractJDBCRowStore[K](pool, table, poolSize, shutdownTimeout, fetchSize, typeMapping) with 
    Store[K, Row] with
    IterableStore[K, Row] with
    QueryableStore[SQLSyntax, (K, Row)] {

  val cleanedKeyColumnName = filterName(keyColumnName)
  
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
        kv._2.map { _ =>
          execute(db, sqlInsertStatement, kv._2)
        }
      }
    }
  }

  protected def get(k: K, session: DBSession): Option[Row] = {
    implicit val sess = session
    sql"SELECT * FROM ${createdTableName} WHERE key = ${k}".map(rs => Row(rs, rowMapping)).single.apply()
  }
 
  override def resultFromWrappedResultSet(row: WrappedResultSet): (K, Row) = (row.get(keyColumnName), rowMapping(row))
}

/**
 * A store using composite keys based on HLists (i.e. composite indexes)
 */
class JDBCMultiKeyRowStore[K <: HList, S <: HList](
    override val pool: ConnectionPool,
    override val table: String,
    keyColumnNames: List[String] = List("key"),
    keySerializers: S,
    override val typeMapping: Option[JDBCToScalaTypesConfig] = None, // None means default type mapping
    override val poolSize: Int = 10,
    override val shutdownTimeout: Duration = Duration.fromSeconds(120),
    override val fetchSize: Int = 500)(implicit
        val a2cSelect: Append2JDBCComposite[StatefulSelect, K, S],
        val a2cUpdate: Append2JDBCComposite[StatefulUpdate, K, S],
        val a2cDelete: Append2JDBCComposite[StatefulDelete, K, S],
        val a2cInsert: Append2JDBCComposite[StatefulInsert, K, S],
        val ft: FromTraversable[K]
        ) extends 
    AbstractJDBCRowStore[K](pool, table, poolSize, shutdownTimeout, fetchSize) with 
    Store[K, Row] with
    IterableStore[K, Row] with
    QueryableStore[SQLSyntax, (K, Row)] {
  
  override def put(kv: (K, Option[Row])): Future[Unit] = futurePool {
    DB(pool.borrow()) localTx { implicit db =>
      get(kv._1, db).map { row =>
        val stmt = kv._2 match {
          case Some(row) =>
            val stateful = StatefulUpdate.update(sqls"${createdTableName}")
            append2composite(stateful, kv._1, keyColumnNames, keySerializers)
            row.columns.
              // filterNot(colval => keyColumnNames.contains(colval._1)).
              foreach(colval => stateful.set(sqls"${SQLSyntax.createUnsafely(colval._1)}", sqls"${colval._2}"))
            stateful.build
          case None => 
            val stateful = StatefulDelete.from(sqls"${createdTableName}")
            append2composite(stateful, kv._1, keyColumnNames, keySerializers)
            stateful.build
        }
        sql"${stmt}".update.apply()
      }.getOrElse {
        kv._2.map { row =>
          val stateful = StatefulInsert.into(sqls"${createdTableName}")
          row.columns.foreach(colval => stateful.add(sqls"${SQLSyntax.createUnsafely(colval._1)}", sqls"${colval._2}"))
          sql"${stateful.build}".update.apply()
        }
      }
    }
  }

  override protected def get(k: K, session: DBSession): Option[Row] = {
    implicit val sess = session
    val stmt = append2composite(StatefulSelect.from(sqls"${createdTableName}"), k, keyColumnNames, keySerializers)
    sql"${stmt.build}".map(rs => Row(rs, rowMapping)).single.apply()
  }
  
  override def resultFromWrappedResultSet(row: WrappedResultSet): (K, Row) = {
    val resultRow = rowMapping(row)
    (HandleComposites.transformRowToKey(resultRow, keyColumnNames).get, rowMapping(row))
  }
}