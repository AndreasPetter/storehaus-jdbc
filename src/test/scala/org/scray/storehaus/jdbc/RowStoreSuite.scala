/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scray.storehaus.jdbc

import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scalikejdbc.ConnectionPool
import scalikejdbc._
import scalikejdbc.scalikejdbcSQLInterpolationImplicitDef
import scalikejdbc.config.DBs
import org.scray.storehaus.jdbc.types.Row
import com.twitter.util.Await
import scalikejdbc.GlobalSettings
import scalikejdbc.SQLFormatterSettings
import scalikejdbc.LoggingSQLAndTimeSettings
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.h2.tools.Server
import scalikejdbc.ConnectionPoolContext
import java.io.File
import org.scray.storehaus.jdbc.types.JDBCToScalaTypesConfig
import scala.util.Random
import com.twitter.util.Future
import com.twitter.concurrent.Spool
import scala.annotation.tailrec

/**
 * Tests for the JDBCRowStore
 * 
 * Using H2 in-memory didn't work as expected so this works on files
 */
@RunWith(classOf[JUnitRunner])
class RowStoreSuite extends WordSpec with BeforeAndAfterAll with LazyLogging {

  
  lazy val sinleconn = {
    ConnectionPool.singleton("jdbc:h2:" + System.getProperty("user.home") + "/storehaus-jdbc/bla.db", "user", "pass")
    ConnectionPool()
  }
  
  override def beforeAll() {
    val dir = new File(System.getProperty("user.home") + "/storehaus-jdbc/")
    if(!dir.exists()) {
      dir.mkdir()
    }
    val files = new File(System.getProperty("user.home") + "/storehaus-jdbc/").listFiles()
    files.foreach { file => 
      if(file.getName.startsWith("bla.db")) {
        file.delete()
      }
    }
    Class.forName("org.h2.Driver")
    GlobalSettings.loggingSQLErrors = true
    using(DB(sinleconn.borrow())) { db => 
      db autoCommit {
        implicit session =>
        sql"CREATE TABLE BLA (key VARCHAR(200) PRIMARY KEY, BLUBBERWUBBER INTEGER, WURZ VARCHAR(50))".execute().apply()
      }
    }
  }
  
  def internalDelete(store: JDBCRowStore[String]) = {
    Await.result(store.put(("Test", None)))
  }
  
  "JDBC-Row-Store" should {
    
    "instantiate JDBC-Row-Store" in {
      
      val jdbcStore = new JDBCRowStore[String](sinleconn, "BLA")
      assert(jdbcStore != null)
      jdbcStore.close()
    }
    
    "put (INSERT) and get some values into JDBC-Row-Store" in {
      // H2 doesn't allow to use the map feature, so we explicitly disable mapping
      val jdbcStore = new JDBCRowStore[String](sinleconn, "BLA", typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      val key = "Test"
      val value0 = "KEY" -> key
      val value1 = "BLUBBERWUBBER" -> 1
      val value2 = "WURZ" -> "blubb"
      Await.result(jdbcStore.put((key, Some(Row(Map(value0, value1, value2))))))
      assert(Await.result(jdbcStore.get(key)) === Some(Row(Map(value0, value1, value2))))
      internalDelete(jdbcStore)
      jdbcStore.close() 
    }

    "check that what a value we didn't insert isn't there in JDBC-Row-Store" in {
      val jdbcStore = new JDBCRowStore[String](sinleconn, "BLA", typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      assert(Await.result(jdbcStore.get("NONEXISTENCE")) === None)      
      jdbcStore.close()      
    }
    
    "put (UPDATE) another row into JDBC-Row-Store" in {
      // H2 doesn't allow to use the map feature, so we explicitly disable mapping
      val jdbcStore = new JDBCRowStore[String](sinleconn, "BLA", typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      val key = "Test"
      val value0 = "KEY" -> key
      val value1 = "BLUBBERWUBBER" -> 2
      val value2 = "WURZ" -> "flupp"
      Await.result(jdbcStore.put((key, Some(Row(Map(value0, value1, value2))))))
      val value10 = "KEY" -> key
      val value11 = "BLUBBERWUBBER" -> 2
      val value12 = "WURZ" -> "flupp"
      Await.result(jdbcStore.put((key, Some(Row(Map(value10, value11, value12))))))
      assert(Await.result(jdbcStore.get(key)) === Some(Row(Map(value10, value11, value12))))
      internalDelete(jdbcStore)
      jdbcStore.close()      
    }

    "put None (DELETE) into JDBC-Row-Store" in {
      // H2 doesn't allow to use the map feature, so we explicitly disable mapping
      val jdbcStore = new JDBCRowStore[String](sinleconn, "BLA", typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      val key = "Test"
      val value0 = "KEY" -> key
      val value1 = "BLUBBERWUBBER" -> 1
      val value2 = "WURZ" -> "blubb"
      Await.result(jdbcStore.put((key, Some(Row(Map(value0, value1, value2))))))
      Await.result(jdbcStore.put((key, None)))
      assert(Await.result(jdbcStore.get(key)) === None)
      jdbcStore.close()      
    }

    "put a thousand rows into JDBC-Row-Store, check existence and iterate over these with getAll" in {
      // H2 doesn't allow to use the map feature, so we explicitly disable mapping
      val jdbcStore = new JDBCRowStore[String](sinleconn, "BLA", typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      (1 to 1000).map { i: Int =>
        val key = s"Test$i"
        val value0 = "KEY" -> key
        val value1 = "BLUBBERWUBBER" -> Random.nextInt(1000000)
        val value2 = "WURZ" -> "flupp"
        Await.result(jdbcStore.put((key, Some(Row(Map(value0, value1, value2))))))
      }
      // make sure all are in
      val t00 = System.currentTimeMillis()
      (1 to 1000).map { i: Int =>
        val key = s"Test$i"
        val result = Await.result(jdbcStore.get(key))
        assert(result.isDefined && result.get.columns.get("KEY") == Some(s"Test$i"))
      } 
      println(System.currentTimeMillis() - t00)
      // fetch all with a spool
      @tailrec def iterateOverEntries(fsp: Future[Spool[(String, Row)]], count: Int): Int = {
        val sp = Await.result(fsp)
        if(sp.isEmpty) {
          count
        } else {
          iterateOverEntries(sp.tail, count + 1)
        }
      }
      val t0 = System.currentTimeMillis()
      assert(iterateOverEntries(jdbcStore.getAll, 0) === 1000)
      println(System.currentTimeMillis() - t0)
      jdbcStore.close()
    }

    
  }

  override def afterAll() {
    // watch all values by uncommenting this
    // org.h2.tools.Console.main("-url", "jdbc:h2:" + System.getProperty("user.home") + "/storehaus-jdbc/bla.db", "-driver", "org.h2.Driver", "-user", "user", "-password", "pass", "-web", "-browser", "-pg")    
    DBs.closeAll()
  }
}
