package catetl 

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._


object FluidIntegration {
	
	
	
	def fluidIntegration ( sc : org.apache.spark.SparkContext  , sqlContext : org.apache.spark.sql.SQLContext , aggregateformula : org.apache.spark.sql.SchemaRDD , TBFCResult : org.apache.spark.sql.SchemaRDD , payload_Exception_Events_variable : org.apache.spark.sql.SchemaRDD ) : org.apache.spark.sql.SchemaRDD  = {
	
		import sqlContext._ 
		import sqlContext.createSchemaRDD
		
		aggregateformula.registerTempTable("aggregateformula")
		
		TBFCResult.registerTempTable("TBFCResult")
		
		payload_Exception_Events_variable.registerTempTable("payload_Exception_Events_variable")
		
		
		val payload_Exception_Events_Fluid_variables = sqlContext.sql(" SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload  , Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalHours , CriticalDays  , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents , fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,  BadSamplePercentage ,  ActualEngineHours, FluidBadSamplePerSMu  ,  FluidSampleSeverityScore ,  FilterChangePerHours ,  CumulativeFilterChangePerHours   FROM  payload_Exception_Events_variable LEFT OUTER JOIN aggregateformula  ON  MachSer = Machine_No AND Bin = Bin_Fluid ")

		payload_Exception_Events_Fluid_variables.registerTempTable("payload_Exception_Events_Fluid_variables")

		TBFCResult.registerTempTable("TBFCResult") 

		val payload_Exception_Events_Fluid_variables_TBFC = sqlContext.sql("SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload  , Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalHours , CriticalDays  , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents , fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,  BadSamplePercentage ,  ActualEngineHours, FluidBadSamplePerSMu  ,  FluidSampleSeverityScore ,  FilterChangePerHours ,  CumulativeFilterChangePerHours  FROM payload_Exception_Events_Fluid_variables LEFT OUTER JOIN  TBFCResult ON MachSer = Machine_Fluid AND Bin = Bin_Fluid ")
		
		 payload_Exception_Events_Fluid_variables_TBFC.registerTempTable("payload_Exception_Events_Fluid_variables_TBFC")
	
		payload_Exception_Events_Fluid_variables_TBFC
	}
	
}
