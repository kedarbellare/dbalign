package cc.refectorie.user.kedarb.dbalign.fields

import collection.mutable.{HashSet, ArrayBuffer}
import com.mongodb.casbah.Imports._
import cc.refectorie.user.kedarb.dbalign.misc.KnowledgeBase

/**
 * @author kedarb
 * @since 5/2/11
 */

trait MongoPersistentField extends Field with CanHashPhrase {
  var maxSegmentLength = 0

  def kb: KnowledgeBase

  def fieldColl: MongoCollection

  def addValue(recordClusterId: ObjectId, phrase: Seq[String], isObserved: Boolean): Unit = {
    val id = new ObjectId
    if (isObserved && maxSegmentLength < phrase.size) maxSegmentLength = phrase.size
    val builder = MongoDBObject.newBuilder
    builder += "_id" -> id
    // add words to the indexer
    builder += "phrase" -> phrase.toArray
    builder += "recordId" -> recordClusterId
    val keyListBuilder = MongoDBList.newBuilder
    for (key <- hashKeys(phrase)) keyListBuilder += key
    builder += "hashkeys" -> keyListBuilder.result
    fieldColl += builder.result
  }

  def numValues: Int = fieldColl.count.toInt

  def getValueId(index: Int): ObjectId = fieldColl.find().skip(index).limit(1).next._id.get

  def clearValues: Unit = fieldColl.drop

  def getValuePhrase(id: ObjectId): Seq[String] = {
    val dbo = fieldColl.findOneByID(id).get
    dbo.as[BasicDBList]("phrase").toSeq.map(_.toString)
  }

  def getRecordClusterIdsForValueId(valueId: ObjectId): Seq[ObjectId] = {
    val dbo = fieldColl.findOneByID(valueId)
    if (dbo.isDefined && dbo.get.isDefinedAt("recordId")) Seq(dbo.get.as[ObjectId]("recordId"))
    else Seq.empty
  }

  def getValueIdForRecordClusterId(recordClusterId: ObjectId): Seq[ObjectId] = {
    val buff = new ArrayBuffer[ObjectId]
    for (dbo <- fieldColl.find(Map("recordId" -> recordClusterId))) buff += dbo._id.get
    buff.toSeq
  }

  def getNeighborValues(phrase: Seq[String]): HashSet[ObjectId] = {
    val neighbors = new HashSet[ObjectId]
    // for (key <- hashKeys(phrase); dbo <- fieldColl.find(Map("hashkeys" -> key)))
    for (dbo <- fieldColl.find("hashkeys" $in hashKeys(phrase).toArray))
      neighbors += dbo._id.get
    neighbors
  }

  def simScore(src: ObjectId, dstPhrase: Seq[String]): Double = {
    val dboOpt = fieldColl.findOneByID(src)
    require(dboOpt.isDefined, "value(" + name + ")[" + src + "] not found!")
    phraseSimScore(getValuePhrase(src), dstPhrase)
  }
}

class DefaultMongoPersistentField(val kb: KnowledgeBase, val name: String, val index: Int, val isKey: Boolean,
                                  val simThreshold: Double, val simWeight: Double,
                                  val _hashKeys: (Seq[String]) => HashSet[String] = PhraseHash.unigramWordHash,
                                  val _phraseSimScore: (Seq[String], Seq[String]) => Double = PhraseSimFunc.cosineSimScore)
  extends MongoPersistentField {
  // initially clear values
  val fieldColl = {
    val coll = kb.getColl(name)
    coll.drop
    coll.ensureIndex("recordId")
    coll.ensureIndex("hashkeys")
    coll
  }

  def hashKeys(phrase: Seq[String]): HashSet[String] = _hashKeys(phrase)

  def phraseSimScore(srcPhrase: Seq[String], dstPhrase: Seq[String]): Double = _phraseSimScore(srcPhrase, dstPhrase)
}