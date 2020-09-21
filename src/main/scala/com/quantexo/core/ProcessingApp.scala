package com.quantexo.core

import com.quantexo.utils.JobConfig
import org.apache.spark.sql.SparkSession

object ProcessingApp  {

  def main(args: Array[String])  {

    try {
      import JobConfig._
      var reportFile: String = null
      var writeStatus: Boolean = false
      val spark = SparkSession.builder.appName("FlightData").getOrCreate()
      val core = CoreData(spark)
      val transform = Transformation(core.getFlightDF(), core.getPassDF(),
        JobConfig.getCountryCode(), JobConfig.getCountTogether(),
        JobConfig.getCountryTransit())
      //  val transform = Transformation(core.getFlightDF(), core.getPassDF())

      reportFile = JobConfig.getProcessConfig().getString("q1Out")
      writeStatus = Report(transform.getAggForMonth(), reportFile)
      reportFile = JobConfig.getProcessConfig().getString("q2Out")
      writeStatus = Report(transform.getFrequentTravellers(), reportFile)
      reportFile = JobConfig.getProcessConfig().getString("q3Out")
      writeStatus = Report(transform.getLongTransitCountry(), reportFile)
      reportFile = JobConfig.getProcessConfig().getString("q4Out")
      writeStatus = Report(transform.getFlightsTogether(), reportFile)
      reportFile = JobConfig.getProcessConfig().getString("q5Out")
      writeStatus = Report(transform.travelTogetherBetweenDates(
                           JobConfig.getCountTogether,
                           JobConfig.getFromDate, JobConfig.getToDate), reportFile)

    } catch {
      case ex: Exception =>
        throw ex
    } finally {

    }

  }

}