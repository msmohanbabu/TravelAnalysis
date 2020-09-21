package com.unittest.common

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import com.quantexo.utils.JobConfig
import com.quantexo.core.Transformation
import com.quantexo.core.CoreData
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CoreTestCase extends UnitTest with TestSparkWrapper {

  //  val spark = SparkSession.builder.appName("UnitTestSpark")
  //    .config("spark.master", "local")
  //    .getOrCreate()
  val sparkHandle = sparkSession

  import sparkHandle.implicits._

  val flightCase1 = Seq(
    (1, 100, "AA", "BB", "2019-01-01"),
    (2, 200, "AA", "DD", "2019-02-01"),
    (3, 500, "EE", "FF", "2019-03-01"),
    (2, 400, "GG", "DD", "2019-04-01"),
    (2, 401, "BB", "DD", "2019-04-02"),
    (2, 500, "AA", "EE", "2019-05-01")).toDF("passengerId", "flightId", "from", "to", "date")

  val passengerCase1 = Seq(
    (1, "AAA", "BBB"),
    (2, "XXX", "YYY"),
    (3, "111", "222")).toDF("passId", "firstName", "lastName")
 

  val transformCase = Transformation(flightCase1, passengerCase1,"AA",0,1)

  behavior of "Month Aggregation"

  it should " - Aggregation should be equal to be expected" in {

    val expected = List(Row(1, 1), Row(2, 1), Row(3, 1), Row(4, 2), Row(5, 1))

    verifyAggregation(expected)

    def verifyAggregation(expected: List[Row]) = {

      assertResult(expected)(transformCase.getAggForMonth().collect.toList)
    }

  }

  it should " - Frequent Travellers should be equal to be expected" in {

    val expected = List(Row(2, 4, "XXX", "YYY"), Row(1, 1, "AAA", "BBB"), Row(3, 1, "111", "222"))

    verifyFrequentTravellers(expected)

    def verifyFrequentTravellers(expected: List[Row]) = {

      assertResult(expected)(transformCase.getFrequentTravellers().collect.toList)
    }

  }
  
  it should " - Long Transit count should be equal to expected" in {

    val expected = List(Row(2, 2))

    verifyLongTransit(expected)

    def verifyLongTransit(expected: List[Row]) = {

      assertResult(expected)(transformCase.getLongTransitCountry().collect.toList)
    }

  }
  
   it should " - Passengers travelled together should be equal to expected" in {

    val expected = List(Row(2, 3, 1))

    verifyTravelTogether(expected)

    def verifyTravelTogether(expected: List[Row]) = {

      assertResult(expected)(transformCase.getFlightsTogether().collect.toList)
    }

  }
  
  
  

}
  
 