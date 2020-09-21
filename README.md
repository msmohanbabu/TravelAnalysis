# TravelAnalysis
Flight Data Analysis
This job can be submitted using sparksubmit.sh - configured to run in cluster mode.

All required parameters\thresholds can be configured in application.conf.

All outputs will be writed in the output path configured in application.conf as stated below


 q1Out = "data/output/solution1"
 
 q2Out = "data/output/solution2"
  
 q3Out = "data/output/solution3"
 
 q4Out = "data/output/solution4"
 
 q5Out = "data/output/solution5"


 Q1 and Q2 does not required any count limits to be passed.

 Q3 needs country code and most transit countries as count. These are configured as below

 countryForTransitCheck = "uk"
 countryTransit = 3

 Q4 and Q5 needs a count on travel together

 travelTogetherCount = 3
 fromDate = "2017-01-01"
 toDate = "2017-04-01"

