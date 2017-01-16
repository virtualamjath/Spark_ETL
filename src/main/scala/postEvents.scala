package catetl

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._

object PostEvents {
	
	
	def getSMU_Event( x : Any ) : Option[Double] = {

		try{  Some( (( Math.floor( x.toString.toDouble /100 ) * 100) + 100 ).toDouble  )}
		catch{
				case p: java.lang.NullPointerException => Some( 100.0 )
				case e : java.lang.Exception => Some( 100.0 ) 		
		}
	}

	def postEventsEvalute ( sc : org.apache.spark.SparkContext  , sqlContext : org.apache.spark.sql.SQLContext , finalEventsOutput : org.apache.spark.sql.SchemaRDD  , Payload_Exception_Variables : org.apache.spark.sql.SchemaRDD  ) : org.apache.spark.sql.SchemaRDD  = {
	
		import sqlContext._ 
		import sqlContext.createSchemaRDD
			
		sqlContext.registerFunction("getSMU_Event" , getSMU_Event( _ : Any ))
		
		finalEventsOutput.registerTempTable("finalEventsOutput")
		Payload_Exception_Variables.registerTempTable("Payload_Exception_Variables")
 
		val postEventDataResults = sqlContext.sql("SELECT MACH_SER_NO, StartDate, EndDate, DateValid, Date, Max_SMU, endOvrSpd1, engPreLubeOvr1, grdLvlShut1, brkRealted1, hiOilTemp1, hiFuelWtr1, idlStndRely1, payloadOvr1, TotalAbuseEvents , getSMU_Event(Max_SMU) AS SMU_Event  FROM finalEventsOutput ")

		postEventDataResults.registerTempTable("postEventDataResults")


		val EventsDataSummary = sqlContext.sql( "SELECT  MACH_SER_NO, StartDate, EndDate, SMU_Event , sum(endOvrSpd1) AS Sum_endOvrSpd1 , sum(engPreLubeOvr1) AS Sum_engPreLubeOvr1 , sum(grdLvlShut1 ) AS Sum_grdLvlShut1 , sum(brkRealted1) AS Sum_brkRealted1 , sum(hiOilTemp1) AS Sum_hiOilTemp1 , sum(hiFuelWtr1) AS Sum_hiFuelWtr1 ,  sum(idlStndRely1) AS Sum_idlStndRely1 , sum(payloadOvr1) AS Sum_payloadOvr1 , sum(TotalAbuseEvents) AS Sum_TotalAbuseEvents  FROM  postEventDataResults GROUP BY  MACH_SER_NO, StartDate, EndDate, SMU_Event  " )


		EventsDataSummary.registerTempTable("EventsERCResultVariables")

		val payload_Exception_Events_variable = sqlContext.sql("SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload ,  Avg_Sum_PAYLOADWEIGHT ,  Avg_Sum_SHIFTCOUNT ,  Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_sum_Overload_1_1 , Avg_Sum_TripTime ,  Avg_Sum_Pyld_LdTrvSpd ,  Avg_Sum_FCR_LTRS_MINS ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalPercent_Datalogger ,CriticalHours , CriticalDays , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents FROM Payload_Exception_Variables LEFT OUTER JOIN EventsERCResultVariables ON MachSer = MACH_SER_NO AND Bin = SMU_Event ")

		payload_Exception_Events_variable.registerTempTable("payload_Exception_Events_variable")
	
		payload_Exception_Events_variable
	
	}
	
	
}
