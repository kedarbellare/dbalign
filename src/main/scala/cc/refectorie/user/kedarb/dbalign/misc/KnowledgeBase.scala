package cc.refectorie.user.kedarb.dbalign.misc

import com.mongodb.casbah.MongoConnection
import com.mongodb.casbah.Imports._

/**
 * @author kedarb
 * @since 3/25/11
 */

class KnowledgeBase(val dbName: String, val hostname: String = "localhost", val port: Int = 27017) {
  val mongoConn = MongoConnection(hostname, port)

  def getColl(name: String) = {
    val coll = mongoConn(dbName)(name)
    coll
  }

  def getRawColl(name: String) = {
    val coll = getColl(name)
    coll.ensureIndex(Map("sourceId" -> 1))
    coll.ensureIndex(Map("recordId" -> 1))
    coll.ensureIndex(Map("clusterId" -> 1))
    coll
  }

  def getMentionColl(name: String) = {
    val coll = getColl(name)
    coll.ensureIndex(Map("isRecord" -> 1))
    coll.ensureIndex(Map("source" -> 1))
    coll.ensureIndex(Map("trueCluster" -> 1))
    coll.ensureIndex(Map("predCluster" -> 1))
    coll
  }
}