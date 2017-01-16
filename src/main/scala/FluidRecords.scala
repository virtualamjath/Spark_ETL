package catetl

case class Machine_Fluid (Machine_No : String , Start_Date : java.sql.Date,  End_Date : java.sql.Date, Date_Valid : java.sql.Date)

case class Fluid_Fluid (Serial_No : String , Date : java.sql.Date,  SMU : Double, analysisResult: String, fluidAddedQnty : String, filterChanged: String, fluidChanged: String, primeCmpnt: String )

case class MyTemp_Fluid( Machine_No : Option[String] ,   Start_Date : Option[java.sql.Date], End_Date : Option[java.sql.Date] , Bin : Option[Double] ,  fluidBadSampleCount : Option[Double] ,fluidSampleCount : Option[Double] , fluidSampleSeverity : Option[Double] , sum_fluidAddedQnty : Option[Double] , fluidChangedCount : Option[Double] , filterChangedCount : Option[Double]  , flilterChangeRunningCount : Option[Double] )

case class FluidSummary_final( Machine_Fluid : String ,  Bin_Fluid : Option[Double] , TBFC : Int ) 
