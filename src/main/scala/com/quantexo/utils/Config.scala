package com.quantexo.utils

import com.typesafe.config.ConfigFactory
import scala.util.Properties
import scala.beans.BeanProperty
import com.typesafe.config.Config
 
//class JobConfig(fileNameOption: Option[String] = None) {

object JobConfig extends Serializable {
  @BeanProperty var configuration: Config = _
  @BeanProperty var processConfig: Config = _
  @BeanProperty var countryCode: String = "AA"
  @BeanProperty var countTogether: Int = 1
  @BeanProperty var countryTransit: Int = 1
  @BeanProperty var flightFile:String = null
  @BeanProperty var passFile: String = null
  @BeanProperty var fromDate: String = null
  @BeanProperty var toDate: String = null

   
  try {
    configuration = ConfigFactory.load()
    flightFile = configuration.getConfig("Configuration").getString("input.flight")
    passFile = configuration.getConfig("Configuration").getString("input.passenger")
    processConfig = configuration.getConfig("Configuration.process")
    countryCode = processConfig.getString("countryForTransitCheck")
    countTogether = processConfig.getString("travelTogetherCount").toInt
    countryTransit = processConfig.getString("countryTransit").toInt
    fromDate = processConfig.getString("fromDate")
    toDate = processConfig.getString("toDate")
    
  } catch {
    case ex: Exception => {
      throw ex
    }
  }

}

