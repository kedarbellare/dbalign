package cc.refectorie.user.kedarb.dbalign.params

import collection.mutable.HashMap

/**
 * @author kedarb
 * @since 3/27/11
 */

class HashWeightVec[K](defaultValue: Double = 0.0) extends HashMap[K, Double] {
  override def default(key: K) = defaultValue

  def increment_!(key: K, value: Double): HashWeightVec[K] = {
    val newValue = apply(key) + value
    update(key, newValue)
    this
  }

  def increment_!(that: HashWeightVec[K], scale: Double): HashWeightVec[K] = {
    if (scale != 0.0) for ((key, value) <- that) increment_!(key, value * scale)
    this
  }

  def increment_!(f: (K) => Double): HashWeightVec[K] = {
    for ((key, value) <- this) increment_!(key, f(key))
    this
  }

  def div_!(scale: Double) = {
    for ((key, value) <- this) update(key, value / scale)
    this
  }

  def dot(that: HashWeightVec[K]): Double = {
    var result = 0.0
    if (this.size > that.size) {
      for ((key, value) <- that; thisValue = this(key)) result += value * thisValue
    } else {
      for ((key, value) <- this; thatValue = that(key)) result += value * thatValue
    }
    result
  }

  def project_!(f: (K, Double) => Double) = {
    for ((key, value) <- this) update(key, f(key, value))
    this
  }

  def squaredNorm = {
    var result = 0.0
    for ((key, value) <- this) result += (value * value)
    result
  }

  def norm2 = math.sqrt(squaredNorm)

  def regularize_!(scale: Double) = {
    if (scale != 0.0) for ((key, value) <- this) increment_!(key, value * scale)
    this
  }
}

class DefaultHashWeightVec(val defaultValue: Double = 0.0) extends HashWeightVec[Any](defaultValue)

class CountVec(val defaultValue: Double = 0.0) extends HashWeightVec[String](defaultValue)