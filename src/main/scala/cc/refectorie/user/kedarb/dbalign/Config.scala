package cc.refectorie.user.kedarb.dbalign

/**
 * @author kedarb
 * @since 5/10/11
 */

import reflect.Manifest
import java.util.Properties
import java.io.{File, FileInputStream, InputStream}

/**
 * Very simple configuration file loader based on java properties files.
 * @author sriedel
 */

class Config(val properties: Properties) {
  def this(in: InputStream) {
    this ({
      val props = new Properties
      props.load(in)
      props
    })
  }

  def get[T](key: String)(implicit m: Manifest[T]): T = {
    val value = properties.getProperty(key)
    if (value == null) error("No value associated with key " + key)
    createBySimpleErasureName(m.erasure.getSimpleName, value)
  }

  def get[T](key: String, default: T)(implicit m: Manifest[T]): T = {
    val value = properties.getProperty(key)
    if (value == null) default else createBySimpleErasureName(m.erasure.getSimpleName, value)
  }

  private def createBySimpleErasureName[T](name: String, untrimmed: String): T = {
    val value = untrimmed.trim
    val result = name match {
      case "int" => value.toInt
      case "String" => value
      case "double" => value.toDouble
      case "boolean" => value.toBoolean
      case "File" => new File(value)
      case "FileInputStream" => new FileInputStream(value)
      case x if (x.endsWith("[]")) => {
        val prefix = x.substring(0, x.indexOf('['))
        val split = value.split(",").map(_.trim).map(createBySimpleErasureName(prefix, _))

      }
      case x => error("Can't convert type " + x)
    }
    result.asInstanceOf[T]
  }

  def put[T](key: String, value: T) = {
    properties.put(key, value.asInstanceOf[Object])
  }
}

object ConfigUtil {
  def getStreamFromClassPathOrFile(propfile: String): InputStream = {
    val is: InputStream = getClass.getClassLoader.getResourceAsStream(propfile)
    if (is != null) {
      return is
    } else {
      return new FileInputStream(propfile)
    }
  }
}

object GlobalConfig extends Config(ConfigUtil.getStreamFromClassPathOrFile("dbie.properties"))

