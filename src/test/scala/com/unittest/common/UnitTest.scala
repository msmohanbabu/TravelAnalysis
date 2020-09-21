package com.unittest.common


import org.scalatest.FunSuite
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfter
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

abstract class UnitTest extends AnyFlatSpec  with Matchers with BeforeAndAfter {
  
   
}

trait TestSparkWrapper {

  lazy val sparkSession: SparkSession = 
    SparkSession.builder().master("local").appName("Unit Test Flight Data Processing").getOrCreate()
   
  sparkSession.sparkContext.setLogLevel("ERROR")
    

}