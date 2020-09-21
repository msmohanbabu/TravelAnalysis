package com.quantexo.core

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.catalyst.ScalaReflection


case class Flight(passengerId: Integer, flightId: Integer, from: String, to: String, date: String) 

object FlightSchema {
  def apply = {
    val flightSchema = ScalaReflection.schemaFor[Flight].dataType.asInstanceOf[StructType]
    flightSchema
  }
}

case class Passenger(passengerId: Integer, firstName: String, lastName: String) 

object PassengerSchema {
  def apply = {
    val passengerSchema = ScalaReflection.schemaFor[Passenger].dataType.asInstanceOf[StructType]
    passengerSchema

  }
}

