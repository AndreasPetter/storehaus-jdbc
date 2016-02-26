package org.scray.storehaus.jdbc

import com.twitter.util.FuturePool
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.ThreadPoolExecutor
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.twitter.util.Duration
import scalikejdbc.ConnectionPool
import scala.collection.mutable.ListBuffer
import scalikejdbc._
import com.twitter.storehaus.ReadableStore
import com.twitter.util.Future
import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.IterableStore
import com.twitter.concurrent.Spool

/**
 * General store handling stuff for JDBC stores based ScalikeJDBC
 */
abstract class AbstractJDBCStore[K, V](
    val pool: ConnectionPool,
    val table: String,
    val poolSize: Int = 10,
    val shutdownTimeout: Duration = Duration.fromSeconds(120),
    val fetchSize: Int = 500) 
      extends QueryableStore[SQLSyntax, (K, V)]
      with IterableStore[K, V]
      with LazyLogging {
  
  def filterName(name: String) = name.filterNot { x => x == '"' || x == '`' || x == ''' || x == ';'}
  
  val cleanedTableName = filterName(table)
  
  lazy val createdTableName = SQLSyntax.createUnsafely(cleanedTableName)
  
  val schema = if (cleanedTableName.split("\\.").size > 1) Some(cleanedTableName.split("\\.").head) else None
  val tablename = if (cleanedTableName.split("\\.").size > 1) cleanedTableName.split("\\.")(1) else cleanedTableName

  /**
   * retrieves a column listing to be used with 
   */
  lazy val columns: List[(String, String)] = {
    using(DB(pool.borrow())) { db =>
      val metadata = db.conn.getMetaData
      val rs = metadata.getColumns(null, schema.orNull, tablename, null)
      val buf = new ListBuffer[(String, String)]
      while(rs.next()) {
        val colName = rs.getString(4)
        val colType = rs.getString(6)
        buf += ((colName, colType))
      }
      buf.toList
    }
  }
  
  lazy val cleanedColumns: List[(String, String)] = columns.map(col => (filterName(col._1), col._2))
  
  val futurePool = FuturePool(new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.SECONDS,
  new LinkedBlockingQueue[Runnable](10 * poolSize), new ThreadPoolExecutor.CallerRunsPolicy()))

  // make sure stores are shut down, even when the JVM is going down
  Runtime.getRuntime.addShutdownHook(new Thread {
    override def run: Unit = {
      def forceShutdown: Unit = {
        val droppedTasks = futurePool.executor.shutdownNow()
        logger.warn(s"""Forcing shutdown of futurePool because Timeout
          of ${shutdownTimeout.inMilliseconds} ms has been reached.
          Dropping ${droppedTasks.size} tasks - potential data loss.""")
      }
      futurePool.executor.shutdown()
      try {
        if (!futurePool.executor.awaitTermination(shutdownTimeout.inMilliseconds, TimeUnit.MILLISECONDS)) {
          // time's up. Forcing shutdown
          forceShutdown
        }
      } catch {
        // handle requested cancel
        case e: InterruptedException => forceShutdown
      }
    }
  })
  
  /**
   * implementors must provide a way to transform a Row
   * as it is provided by ScalikeJDBC into a tuple (K, V)
   */
  protected def resultFromWrappedResultSet(row: WrappedResultSet): (K, V)
  
  /**
   * Using resultFromWrappedResultSet it is a task of synchronization to create an
   * Iterator from a result of a query.
   * This complicated stuff is necessary, because we need to transform a foreach
   * loop into an Iterator. This effectively inverts control and gives it to the
   * client of the library instead of using the database for it.
   * TODO: improve creation of iterators further.
   */
  protected def executeQuery(query: Option[SQLSyntax] = None): Iterator[(K, V)] = {
    val sqlsdf = sqls"test"
    var nextElement: Option[(K, V)] = None
    var oldToggle: Boolean = true
    var toggle: Boolean = true
    val syncObject = new Object
    futurePool {
      DB(pool.borrow()) readOnly { implicit db =>
        db.fetchSize(fetchSize)
        val filters = query.getOrElse(sqls"")
        sql"SELECT * FROM ${createdTableName} ${filters}".foreach { row =>
          nextElement = Some(resultFromWrappedResultSet(row))
          toggle = !toggle
          syncObject.synchronized {
            syncObject.wait()
          }
        }
        nextElement = None
        toggle = !toggle
      }
    }
    new Iterator[(K, V)] {
      override def hasNext: Boolean = {
        while(oldToggle == toggle) {
          Thread.sleep(0, 5)
        }
        nextElement.isDefined
      }
      override def next: (K, V) = {
        while(oldToggle == toggle) {
          Thread.sleep(0, 5)
        }
        val ele = nextElement.get
        advance()
        ele
      }
      private def advance() {
        oldToggle = toggle
        syncObject.synchronized {
          syncObject.notify()
        }
      }
    }
  }
  
  /**
   * Query the database and return all information in accordance to the provided
   * filter SQLSyntax
   * 
   * WARNING: clients must be aware that it is the responsibility of the client
   * to provide a query String that is not vulnerable to SQL injection (which should
   * probably be the case if the SQLSyntax has been created using scalikeJDBC's sqls
   * String interpolation feature and no special stuff has been used which is not 
   * covered by sqls.
   */
  override def queryable: ReadableStore[SQLSyntax, Seq[(K, V)]] = new ReadableStore[SQLSyntax, Seq[(K, V)]] {
    override def get(query: SQLSyntax): Future[Option[Seq[(K, V)]]] = futurePool {
      val it = executeQuery(Some(query))
      it.hasNext match {
        case false => None
        case true => Some(it.toSeq.view)
      }
    }
  }
  
  /**
   * implements IterableStore which can do a full table scan
   */
  override def getAll: Future[Spool[(K, V)]] =  {
    val it = executeQuery()
    IterableStore.iteratorToSpool(it)
  }

}