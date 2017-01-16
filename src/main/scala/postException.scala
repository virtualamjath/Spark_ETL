package catetl

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._


object PostException {
	
	def exception_Bin( x : Any ) : Option[Double] = {
		try{  
				Some( ( Math.floor( x.toString.toDouble/100 ) * 100 ) + 100  ) 
		}
		catch{ 
				case e : java.lang.NullPointerException  => None 		
				case e : java.lang.Exception => None 
		}
	}
	
	
	def dayDiff_Exception ( day1 : Any , day2 : Any ) : Option[Double] = {
		
		try { 
				Some( (java.sql.Date.valueOf( day1.toString ).getTime - java.sql.Date.valueOf( day1.toString ).getTime ).toDouble / 24 * 60 * 60 * 1000  ) 
		}
		catch {
				case e : java.lang.NullPointerException => None 
				case e : java.lang.Exception => None
		}	
	}
	
	
	def getRatio( x : Any , y : Any  ) : Option[Double] = {

		try{
				if( y.toString.toDouble == 0.0 )
					None			
				else 
					Some( x.toString.toDouble / y.toString.toDouble )
			
		}
		catch {
			case e : Exception => Some( 0.0 )	
		}

	}
	

	
	def postExceptionEvalution ( sc : org.apache.spark.SparkContext  , sqlContext : org.apache.spark.sql.SQLContext , leftJoined : org.apache.spark.sql.SchemaRDD  , payloadSummary_formaula : org.apache.spark.sql.SchemaRDD , payload_Result_Variables : org.apache.spark.sql.SchemaRDD ) : org.apache.spark.sql.SchemaRDD  = {
			
			import sqlContext._ 
			import sqlContext.createSchemaRDD
			
			leftJoined.registerTempTable("ExceptionResult")
			payloadSummary_formaula.registerTempTable("payloadSummary_formaula")
			payload_Result_Variables.registerTempTable("payload_Result_Variables")
			sqlContext.registerFunction( "exception_Bin" , exception_Bin( _ : Any ) )

			val ExceptionJoinedPayload = sqlContext.sql( "SELECT  machineNo ,startDate, dateValid ,Critical_Fluid ,Critical_Event , Critical_Trend , Critical_Datalogger , Critical_All , Fluid , Event , Trend , Datalogger , Date , Critical_Fluid2 , Critical_Event2 , Critical_Trend2 ,  Critical_Datalogger2 , Critical_All2 , exception_Bin( SMU_payload ) As SMU_Exception , SMU_payload FROM ExceptionResult JOIN payloadSummary_formaula ON  machineNo =  Machine_Ser AND dateValid = DateValid " )

			ExceptionJoinedPayload.registerTempTable("ExceptionJoinedPayload")

			ExceptionJoinedPayload.cache()

			val ExceptionJoinsBins = sqlContext.sql("SELECT MachSer , SMU_valid , TID , EngineReplacementSMU , startDate, dateValid ,Critical_Fluid ,Critical_Event , Critical_Trend , Critical_Datalogger , Critical_All , Fluid , Event , Trend , Datalogger , Critical_Fluid2 , Critical_Event2 , Critical_Trend2 ,  Critical_Datalogger2 , Critical_All2 , SMU_Exception , SMU_payload  FROM payloadBinsJoin1 LEFT OUTER JOIN ExceptionJoinedPayload ON MachSer = machineNo AND SMU_valid = SMU_Exception ")


			ExceptionJoinsBins.registerTempTable("ExceptionJoinsBins")

			val ExceptionSummary = sqlContext.sql("SELECT MachSer , SMU_valid , TID , EngineReplacementSMU , sum( Critical_Fluid ) As Sum_Critical_Fluid , sum(Critical_Event) As Sum_Critical_Event , sum(Critical_Trend) As Sum_Critical_Trend , sum(Critical_Datalogger) As Sum_Critical_Datalogger , sum(Critical_All) AS Sum_Critical_All , Sum(Event ) As Sum_Event , sum(Fluid) As Sum_Fluid , sum( Trend ) As Sum_Trend , sum(Datalogger) As Sum_Datalogger , min(dateValid) As Min_Date_exception , max( dateValid ) AS Max_Date_exception  , min(SMU_payload) AS min_SMU_payload_Exception , max(SMU_payload) AS max_SMU_payload_Exception    FROM  ExceptionJoinsBins GROUP BY MachSer , SMU_valid , TID , EngineReplacementSMU  ")


			ExceptionSummary.registerTempTable("ExceptionSummary")
			
			
			sqlContext.registerFunction("dayDiff_Exception" , dayDiff_Exception( _ : Any , _ : Any ) )
			
			sqlContext.registerFunction("getRatio" , getRatio( _ : Any , _ : Any ) ) 

			//val ExceptionERCVariables = sqlContext.sql("SELECT MachSer AS Mach_E , SMU_valid AS SMU_E  , TID AS TID_E ,  (Sum_Critical_Trend / Sum_Trend) As CriticalPercent_Trend , ( Sum_Critical_Event / Sum_Event   ) AS CriticalPercent_Event , ( Sum_Critical_Fluid / Sum_Fluid  ) As CriticalPercent_Fluid , (Sum_Critical_Datalogger / Sum_Datalogger ) As CriticalPercent_Datalogger , ( Sum_Critical_All / ( max_SMU_payload_Exception - min_SMU_payload_Exception ) )  AS CriticalHours , ( Sum_Critical_All / ( dayDiff_Exception( Max_Date_exception , Min_Date_exception ) ) ) AS CriticalDays FROM ExceptionSummary  ")
			
			 val ExceptionERCVariables = sqlContext.sql("SELECT MachSer AS Mach_E , SMU_valid AS SMU_E  , TID AS TID_E ,  getRatio(Sum_Critical_Trend , Sum_Trend) As CriticalPercent_Trend , getRatio( Sum_Critical_Event , Sum_Event   ) AS CriticalPercent_Event , getRatio( Sum_Critical_Fluid , Sum_Fluid  ) As CriticalPercent_Fluid , getRatio(Sum_Critical_Datalogger , Sum_Datalogger ) As CriticalPercent_Datalogger , getRatio( Sum_Critical_All , ( max_SMU_payload_Exception - min_SMU_payload_Exception ) )  AS CriticalHours , getRatio( Sum_Critical_All , ( dayDiff_Exception( Max_Date_exception , Min_Date_exception ) ) ) AS CriticalDays FROM ExceptionSummary ORDER BY Mach_E , SMU_E ")

			ExceptionERCVariables.registerTempTable( "ExceptionERCVariables" )


			val Payload_Exception_Variables = sqlContext.sql(" SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload ,  Avg_Sum_PAYLOADWEIGHT ,  Avg_Sum_SHIFTCOUNT ,  Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_sum_Overload_1_1 , Avg_Sum_TripTime ,  Avg_Sum_Pyld_LdTrvSpd ,  Avg_Sum_FCR_LTRS_MINS ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalPercent_Datalogger ,CriticalHours , CriticalDays FROM payload_Result_Variables LEFT OUTER JOIN ExceptionERCVariables ON   MachSer = Mach_E AND SMU_E = Bin  AND TID = TID_E ")

			Payload_Exception_Variables.registerTempTable("Payload_Exception_Variables")

			Payload_Exception_Variables
	}
	
		
	
	}
