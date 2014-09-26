package org.locationtech.geomesa.analytic

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics
import org.joda.time.{DateTimeZone, Interval, DateTime}

class TimeSeries(interval: TimeInterval = DayInterval,
                 window: Int = 5,
                 sigmaSensivity: Double = 2.8,
                 saveFunc: (TimeSeriesData) => Unit)
  extends Logging with Serializable {

  private val timeUnits = collection.mutable.ListBuffer.empty[DateTime]
  private val fullStats = new DescriptiveStatistics(window)
  private var mostRecentUnitStats: DescriptiveStatistics = null

  private var curUnit: DateTime = null
  private var curUnitAlerted = false
  private var lastAlertId: String = null
  private val obs = collection.mutable.ListBuffer.empty[UnitCount]

  class UnitCount(val unit: DateTime) {
    private var count = 0
    def getCount() = count
    def increment(by: Int = 1) = count += by
  }

  def addObs(unit: DateTime, numObs: Int): (Boolean, Option[String]) = {
    if (curUnit == null) {
      startUnit(unit)
    } else if (!interval.sameInterval(unit, curUnit)) {
      do {
        val tmp = curUnit
        endUnit()
        startUnit(interval.nextInterval(tmp))
      }
      while(!interval.sameInterval(unit, curUnit))
    }

    val isAlert = doAddObs(numObs)
    val alertIdOpt =
      if (isAlert) {
        lastAlertId = UUID.randomUUID().toString
        Some(lastAlertId)
      }
      else {
        None
      }

    (isAlert, alertIdOpt)
  }

  def startUnit(time: DateTime) = {
    timeUnits += time
    curUnit = interval.timeInterval(time)
    println(curUnit.toDate.toGMTString)
    obs += new UnitCount(curUnit)
  }

  private def doAddObs(numObs: Int = 1): Boolean = {
    if (curUnit == null) throw new IllegalStateException("startUnit() before adding obs")

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

  private def endUnit() = {
    saveFunc(TimeSeriesData(curUnit, obs.last.getCount(), curUnitAlerted, Option(lastAlertId)))
    resetUnitState()
    updateStats()
  }

  private def resetUnitState() = {
    curUnit = null
    curUnitAlerted = false
    lastAlertId = null
  }

  private def updateStats() = {
    fullStats.addValue(obs.last.getCount())
    mostRecentUnitStats = fullStats.copy()
  }

  def getMean = mostRecentUnitStats.getMean

  def getStandardDeviation = mostRecentUnitStats.getStandardDeviation

  def getDayDiff = obs.last.getCount - getMean

  def getSigma = getDayDiff

  def getLastCount = obs.last.getCount()

  def getData: Seq[(DateTime, Int)] = timeUnits.zip(mostRecentUnitStats.getValues.map(_.toInt))
}

case class TimeSeriesData(dt: DateTime, count: Int, alert: Boolean, alertId: Option[String])

// TODO use?
case class UnitState(curUnit: DateTime = null, curUnitAlerted: Boolean = false, lastAlertId: String = null)

// TODO functional programming on TimeInterval implementation with* function chaining
sealed trait TimeInterval {
  def timeInterval(d: DateTime): DateTime
  def sameInterval(d1: DateTime, d2: DateTime) = timeInterval(d1) == timeInterval(d2)
  def newDay = new DateTime(0).withZone(DateTimeZone.forID("GMT"))
  def nextInterval(d: DateTime): DateTime
}

object HourInterval extends TimeInterval {
  override def timeInterval(d: DateTime) =
    newDay.
      withYear(d.getYear).
      withDayOfYear(d.getDayOfYear).
      withHourOfDay(d.getHourOfDay)

  def nextInterval(d: DateTime) = timeInterval(d).plusHours(1)

}

object DayInterval extends TimeInterval {
  override def timeInterval(d: DateTime) =
    newDay.
      withYear(d.getYear).
      withDayOfYear(d.getDayOfYear)

  def nextInterval(d: DateTime) = timeInterval(d).plusDays(1)
}

object MinuteInterval extends TimeInterval {
  override def timeInterval(d: DateTime) =
    newDay.
      withYear(d.getYear).
      withDayOfYear(d.getDayOfYear).
      withHourOfDay(d.getHourOfDay).
      withMinuteOfHour(d.getMinuteOfDay)

  def nextInterval(d: DateTime) = timeInterval(d).plusMinutes(1)
}

object SecondInterval extends TimeInterval {
  override def timeInterval(d: DateTime) =
    newDay.
      withYear(d.getYear).
      withDayOfYear(d.getDayOfYear).
      withHourOfDay(d.getHourOfDay).
      withMinuteOfHour(d.getMinuteOfHour).
      withSecondOfMinute(d.getSecondOfMinute)

  def nextInterval(d: DateTime) = timeInterval(d).plusSeconds(1)
}
