package cc.refectorie.user.kedarb.dbalign.misc

import org.apache.log4j.Logger

/**
 * @author kedarb
 * @since 3/25/11
 */

trait HasLogger {
  def logger: Logger

  def info(msg: Any) = logger.info(msg)

  def error(msg: Any) = logger.error(msg)

  def debug(msg: Any) = logger.debug(msg)

  def warn(msg: Any) = logger.warn(msg)
}