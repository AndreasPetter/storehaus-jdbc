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
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import org.scalatest.junit.JUnitRunner
import scalikejdbc._
import scalikejdbc.config.DBs
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.h2.tools.Server
import java.io.File
import org.scray.storehaus.jdbc.types.{JDBCToScalaTypesConfig, Row}
import scala.util.Random
import com.twitter.util.{Await, Future}
import com.twitter.concurrent.Spool
import scala.annotation.tailrec
import shapeless._
import shapeless.syntax.std.traversable._
import shapeless.ops.traversable._
import HandleComposites._
import ch.qos.logback.classic.{Level, Logger}
import org.slf4j.{Logger => SL4JLogger, LoggerFactory}

/**
 * Tests for the JDBCRowStore
 * 
 * Using H2 in-memory didn't work as expected so this works on files
 */
@RunWith(classOf[JUnitRunner])
class RowStoreSuite extends WordSpec with BeforeAndAfterAll with LazyLogging {

  type KEY_TYPE = String :: Int :: HNil
  type SERIALIZER_TYPE = TypeBinder[String] :: TypeBinder[Int] :: HNil
  val serializers = implicitly[TypeBinder[String]] :: implicitly[TypeBinder[Int]] :: HNil
  
  LoggerFactory.getLogger(SL4JLogger.ROOT_LOGGER_NAME).asInstanceOf[Logger].setLevel(Level.INFO)

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
        sql"CREATE TABLE BLUBB (KEY VARCHAR(200), BLUBBERWUBBER INTEGER, WURZ VARCHAR(50), PRIMARY KEY (KEY, BLUBBERWUBBER))".execute().apply()
      }
    }
  }
  
  def internalDelete(store: JDBCRowStore[String]) = {
    Await.result(store.put(("Test", None)))
  }
  
  def internalDelete(store: JDBCMultiKeyRowStore[KEY_TYPE, SERIALIZER_TYPE]) = {
    Await.result(store.put(("Test" :: 1 :: HNil, None)))
  }

  @tailrec final def iterateOverEntries[T](fsp: Future[Spool[(T, Row)]], count: Int): Int = {
    val sp = Await.result(fsp)
    if(sp.isEmpty) {
      count
    } else {
      iterateOverEntries(sp.tail, count + 1)
    }
  }

  
  "Single key JDBC-Row-Store" should {
    
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
    
    "put (UPDATE) another row into the JDBC-Row-Store" in {
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

    "put None (DELETE) into the JDBC-Row-Store" in {
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
      val number = 1000
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
      logger.info(s"Single-key Rowstore: Fetching $number entries by executing $number queries took ${(System.currentTimeMillis() - t00)} ms.")
      // fetch all with a spool
      val t0 = System.currentTimeMillis()
      assert(iterateOverEntries(jdbcStore.getAll, 0) === 1000)
      logger.info(s"Single-key Rowstore: Iterating over $number entries took ${(System.currentTimeMillis() - t0)} ms.")
      jdbcStore.close()
    }
  }

  "Multi-key JDBC-Row-Store" should {

    
    "instantiate JDBC-Row-Store" in {
      val jdbcStore = new JDBCMultiKeyRowStore[KEY_TYPE, SERIALIZER_TYPE](sinleconn, "BLUBB", List("KEY", "BLUBBERWUBBER"), serializers, typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      assert(jdbcStore != null)
      jdbcStore.close()
    }
    
    "put (INSERT) and get some values into JDBC-Row-Store" in {
      // H2 doesn't allow to use the map feature, so we explicitly disable mapping
      val jdbcStore = new JDBCMultiKeyRowStore[KEY_TYPE, SERIALIZER_TYPE](sinleconn, "BLUBB", List("KEY", "BLUBBERWUBBER"), serializers, typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      val key = "Test" :: 1 :: HNil
      val value0 = "KEY" -> "Test"
      val value1 = "BLUBBERWUBBER" -> 1
      val value2 = "WURZ" -> "blubb"
      Await.result(jdbcStore.put((key, Some(Row(Map(value0, value1, value2))))))
      assert(Await.result(jdbcStore.get(key)) === Some(Row(Map(value0, value1, value2))))
      internalDelete(jdbcStore)
      jdbcStore.close() 
    }

    "check that a value we didn't insert isn't there in JDBC-Row-Store" in {
      val jdbcStore = new JDBCMultiKeyRowStore[KEY_TYPE, SERIALIZER_TYPE](sinleconn, "BLUBB", List("KEY", "BLUBBERWUBBER"), serializers, typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      assert(Await.result(jdbcStore.get("TestBrummelchen" :: 15 :: HNil)) === None)      
      jdbcStore.close()      
    }
    
    "put (UPDATE) another row into the JDBC-Row-Store" in {
      // H2 doesn't allow to use the map feature, so we explicitly disable mapping
      val jdbcStore = new JDBCMultiKeyRowStore[KEY_TYPE, SERIALIZER_TYPE](sinleconn, "BLUBB", List("KEY", "BLUBBERWUBBER"), serializers, typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      val key = "Test" :: 1 :: HNil
      val value0 = "KEY" -> "Test"
      val value1 = "BLUBBERWUBBER" -> 1
      val value2 = "WURZ" -> "blubb"
      Await.result(jdbcStore.put((key, Some(Row(Map(value0, value1, value2))))))
      val value10 = "KEY" -> "Test"
      val value11 = "BLUBBERWUBBER" -> 1
      val value12 = "WURZ" -> "flupp"
      Await.result(jdbcStore.put((key, Some(Row(Map(value10, value11, value12))))))
      assert(Await.result(jdbcStore.get(key)) === Some(Row(Map(value10, value11, value12))))
      internalDelete(jdbcStore)
      jdbcStore.close()      
    }

    "put None (DELETE) into the JDBC-Row-Store" in {
      // H2 doesn't allow to use the map feature, so we explicitly disable mapping
      val jdbcStore = new JDBCMultiKeyRowStore[KEY_TYPE, SERIALIZER_TYPE](sinleconn, "BLUBB", List("KEY", "BLUBBERWUBBER"), serializers, typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      val key = "Test" :: 1 :: HNil
      val value0 = "KEY" -> "Test"
      val value1 = "BLUBBERWUBBER" -> 1
      val value2 = "WURZ" -> "blubb"
      Await.result(jdbcStore.put((key, Some(Row(Map(value0, value1, value2))))))
      Await.result(jdbcStore.put((key, None)))
      assert(Await.result(jdbcStore.get(key)) === None)
      jdbcStore.close()      
    }

    "put a thousand rows into JDBC-Row-Store, check existence and iterate over these with getAll" in {
      // H2 doesn't allow to use the map feature, so we explicitly disable mapping
      val jdbcStore = new JDBCMultiKeyRowStore[KEY_TYPE, SERIALIZER_TYPE](sinleconn, "BLUBB", List("KEY", "BLUBBERWUBBER"), serializers, typeMapping = Some(JDBCToScalaTypesConfig(None, true)))
      val number = 1000 
      (1 to number).map { i: Int =>
        val key = s"Test$i" :: i :: HNil
        val value0 = "KEY" -> s"Test$i"
        val value1 = "BLUBBERWUBBER" -> i
        val value2 = "WURZ" -> "flupp"
        Await.result(jdbcStore.put((key, Some(Row(Map(value0, value1, value2))))))
      }
      // make sure all are in
      val t00 = System.currentTimeMillis()
      (1 to number).map { i: Int =>
        val key = s"Test$i" :: i :: HNil
        val result = Await.result(jdbcStore.get(key))
        assert(result.isDefined && result.get.columns.get("KEY") == Some(s"Test$i"))
      } 
      logger.info(s"Multi-key Rowstore: Fetching $number entries by executing $number queries took ${(System.currentTimeMillis() - t00)} ms.")
      // fetch all with a spool
      val t0 = System.currentTimeMillis()
      assert(iterateOverEntries(jdbcStore.getAll, 0) === number)
      logger.info(s"Multi-key Rowstore: Iterating over $number entries took ${(System.currentTimeMillis() - t0)} ms.")
      jdbcStore.close()
    }   
  }
  
  override def afterAll() {
    // watch all values by uncommenting this
    // org.h2.tools.Console.main("-url", "jdbc:h2:" + System.getProperty("user.home") + "/storehaus-jdbc/bla.db", "-driver", "org.h2.Driver", "-user", "user", "-password", "pass", "-web", "-browser", "-pg")    
    DBs.closeAll()
  }
}
