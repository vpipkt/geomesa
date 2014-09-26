package org.locationtech.geomesa.analytic

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics
import org.joda.time.DateTime

class TimeSeries(interval: TimeInterval = DayInterval,
                 window: Int = 5,
                 sigmaSensivity: Double = 2.8,
                 storeData: (TimeSeriesData) => Unit)
  extends Logging with Serializable {

  private val timeUnits = collection.mutable.ListBuffer.empty[DateTime]
  private val fullStats = new DescriptiveStatistics(window)
  private var mostRecentUnitStats: DescriptiveStatistics = null

  private var curUnit: DateTime = null
  private var curUnitAlerted = false
  private val obs = collection.mutable.ListBuffer.empty[UnitCount]

  class UnitCount(val unit: DateTime) {
    private var count = 0
    def getCount() = count
    def increment(by: Int = 1) = count += by
  }

  def addObs(unit: DateTime, numObs: Int): (Boolean, Option[TimeSeriesData]) = {
    val lastData: Option[TimeSeriesData] =
      if (curUnit == null) {
        startUnit(unit)
        None
      } else if (!interval.sameInterval(unit, curUnit)) {
        val ret = endUnit()
        startUnit(unit)
        Some(ret)
      } else None

    val isAlert = addObs(numObs)
    (isAlert, lastData)
  }



  def startUnit(time: DateTime) = {
    timeUnits += time
    curUnit = interval.timeInterval(time)
    obs += new UnitCount(curUnit)
  }

  def addObs(numObs: Int = 1): Boolean = {
    // If not in a current unit...bad user
    if (curUnit == null)
      throw new IllegalStateException("Must call startUnit() before adding obs to current unit")

    // startDay() should have added something to obs...take the most recent
    obs.last.increment(numObs)

    // deep copy previous stats then increment today by the last obs
    mostRecentUnitStats = fullStats.copy()
    mostRecentUnitStats.addValue(obs.last.getCount())

    // return true if obs caused an alert and we haven't generated one yet
    val alertGenerated = getSigma > sigmaSensivity
    if (alertGenerated && !curUnitAlerted) {
      curUnitAlerted = true
      true
    } else {
      false
    }
  }

  def endUnit(): TimeSeriesData = {
    logger.trace("\tClosing "+curUnit+" with count " + obs.last.getCount())
    val ret = TimeSeriesData(curUnit, obs.lastOption.get.getCount(), curUnitAlerted)
    curUnit = null
    curUnitAlerted = false

    obs.lastOption.map { dc => fullStats.addValue(dc.getCount()) }
    mostRecentUnitStats = fullStats.copy()
    ret
  }

  def getMean = mostRecentUnitStats.getMean

  def getStandardDeviation = mostRecentUnitStats.getStandardDeviation

  def getDayDiff = obs.last.getCount - getMean

  def getSigma = getDayDiff

  def getLastCount = obs.last.getCount()

  def getData: Seq[(DateTime, Int)] = timeUnits.zip(mostRecentUnitStats.getValues.map(_.toInt))
}

case class TimeSeriesData(dt: DateTime, count: Int, alert: Boolean, alertId: Option[String])

// TODO functional programming on TimeInterval implementation with* function chaining
sealed trait TimeInterval {
  def timeInterval(d: DateTime): DateTime
  def sameInterval(d1: DateTime, d2: DateTime) = timeInterval(d1) == timeInterval(d2)
  def newDay = new DateTime(0)
}

object HourInterval extends TimeInterval {
  override def timeInterval(d: DateTime) =
    newDay.
      withYear(d.getYear).
      withDayOfYear(d.getDayOfYear).
      withHourOfDay(d.getHourOfDay)
}

object DayInterval extends TimeInterval {
  override def timeInterval(d: DateTime) =
    newDay.
      withYear(d.getYear).
      withDayOfYear(d.getDayOfYear)
}

object MinuteInterval extends TimeInterval {
  override def timeInterval(d: DateTime) =
    newDay.
      withYear(d.getYear).
      withDayOfYear(d.getDayOfYear).
      withHourOfDay(d.getHourOfDay).
      withMinuteOfHour(d.getMinuteOfDay)
}

object SecondInterval extends TimeInterval {
  override def timeInterval(d: DateTime) =
    newDay.
      withYear(d.getYear).
      withDayOfYear(d.getDayOfYear).
      withHourOfDay(d.getHourOfDay).
      withMinuteOfHour(d.getMinuteOfHour).
      withSecondOfMinute(d.getSecondOfMinute)
}
