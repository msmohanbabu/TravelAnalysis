//package com.quantexo.core
//
//import org.apache.spark.sql.SparkSession
//import com.quantexo.utils.JobConfig
//
//object App {
//    
//  def main(args : Array[String]) {
//   
//  val spark = SparkSession.builder.appName("FlightData").getOrCreate()
//    
//   val myconfig =  JobConfig()
//   
//   val flightFile = myconfig.getString("files.input.flight")
//   val passengerFile = myconfig.getString("files.input.passenger")
//   
//   val flightData = spark.read.option("header", "true")
//                    .schema("passengerId Integer, flightId Integer, from String, to String, date String")
//                    .csv(flightFile)
//                    
//   val passengerData = spark.read.option("header", "true")
//                      .schema("passengerId Integer, firstName String, lastName String")
//                      .csv(passengerFile)         
//
//  }
//
//}
