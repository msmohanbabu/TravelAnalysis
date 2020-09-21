package com.quantexo.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.beans.BeanProperty
import org.apache.spark.sql.expressions.Window
import com.quantexo.utils.JobConfig


class Transformation(flightData: DataFrame,PassengerData: DataFrame,
                     countryCode:String,countTogether:Int,countryTransitCount:Int) extends Serializable {
  @BeanProperty var aggForMonth: DataFrame = _
  @BeanProperty var frequentTravellers: DataFrame = _
  @BeanProperty var longTransitCountry: DataFrame = _
  @BeanProperty var flightsTogether: DataFrame = _
  @BeanProperty var flightDateBDates: DataFrame = _

  //  var country:String = null
  //  var countTogether:String = null

  //  try {
  //    country = jobConfig.getProcessConfig().getString("countryForTransitCheck")
  //    if (country.length() != 2) {
  //      throw new Exception("Invalid CountryCode")
  //    }
  //    countTogether = jobConfig.getProcessConfig().getString("travelTogetherCount")
  //  }
  //  catch {
  //    case ex: NumberFormatException =>
  //      throw new Exception("Invalid Number for Travel Together" + countTogether)
  //    case ex: Exception =>
  //      throw ex
  //  }

  def validateCountries = udf { values: collection.mutable.WrappedArray[String] =>
    val indices = values.toList.zip(Stream from 1).filter(_._1 == countryCode).map(_._2)
    var maxCount = 0
    if (indices.length == 2)
      maxCount = indices(1) - indices(0)
    else if (indices.length > 2)
      maxCount = indices.combinations(2).map { case List(a, b) => b - a }.sliding(2).map { case Seq(x, y, _*) => y - x }.toList.max
    else
      maxCount = indices.max
    maxCount - 1
  }

  try {
    aggForMonth = flightData.withColumn("Month", col("date").substr(6, 2).cast("int"))
      .groupBy("Month").agg(count("passengerId").alias("Number of Flights"))
      .orderBy(col("Month").asc)

  } catch {
    case ex: Exception =>
      throw ex
  }

  try {

    frequentTravellers = PassengerData.join(flightData, PassengerData("passId") === flightData("passengerId")).drop("passId")
      .groupBy("passengerId", "firstName", "lastName").agg(count("passengerId").alias("Number of Flights"))
      .sort(col("Number Of Flights").desc).limit(100).orderBy(col("Number Of Flights").desc)
      .select(col("passengerId").as("Passenger ID"), col("Number Of Flights"),
        col("firstName").as("First Name"), col("lastName").as("Last Name"))

  } catch {
    case ex: Exception =>
      throw ex
  }

  try {
    longTransitCountry = flightData.sort(col("passengerId"), col("date")).groupBy("passengerId")
      .agg(collect_list(col("from")).as("countries"))
      .filter(array_contains(col("countries"), countryCode))
      .withColumn("count", validateCountries(col("countries"))).filter(col("count") > countryTransitCount)
      .select(col("passengerId").alias("Passenger ID"), col("count").alias("Longest Run"))
      .orderBy(col("Passenger ID").asc)

  } catch {
    case ex: Exception =>
      throw ex
  }

  @transient lazy val windowSpec = Window.partitionBy(col("Passenger 1 Id"), col("Passenger 2 Id"))
  try {

    flightsTogether = flightData.as("a")
      .join(flightData.as("b"), (col("a.flightId") === col("b.flightId"))
        && (col("a.passengerId") <= col("b.passengerId")), "inner")
      .filter("a.passengerId != b.passengerId")
      .select(col("a.passengerId").as("Passenger 1 Id"), col("b.passengerId").as("Passenger 2 Id"))
      .withColumn("Number of flights together", count(col("Passenger 1 Id")).over(windowSpec)).distinct
      .orderBy(col("Number of flights together").desc).filter(col("Number of flights together") > countTogether)

  } catch {
    case ex: Exception =>
      throw ex
  }
  
    def travelTogetherBetweenDates(atleastCount: Int, fromDate: String, toDate: String,all:Boolean=true) = {

    val filterCondition = String.format("date between \'%s\' and \'%s\'", fromDate, toDate)
    flightDateBDates = flightData.filter(filterCondition)

    val travelBetweenDates = flightDateBDates.as("a")
      .join(flightDateBDates.as("b"), (col("a.flightId") === col("b.flightId"))
        && (col("a.passengerId") <= col("b.passengerId")), "inner")
      .filter("a.passengerId != b.passengerId")
      .select(col("a.passengerId").as("Passenger 1 Id"), col("b.passengerId").as("Passenger 2 Id"),col("a.date").as("date"))
      .withColumn("Number of flights together", count(col("Passenger 1 Id")).over(windowSpec)).distinct
      .groupBy("Passenger 1 Id","Passenger 2 Id","Number of flights together")
      .agg(min(col("date").cast("date")).alias("From"),max(col("date").cast("date")).alias("To"))
      .orderBy(col("Number of flights together").desc).filter(col("Number of flights together") > countTogether)

    travelBetweenDates

  }

}

object Transformation {
  def apply(flightData: DataFrame,PassengerData: DataFrame,
             countryCode:String,countTogether:Int,countryTransitCount:Int)  = {

    val transform = new Transformation(flightData, PassengerData, countryCode, countTogether, countryTransitCount)
    transform
  }
  
}