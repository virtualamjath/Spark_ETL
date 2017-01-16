import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive._
import java.util.Properties
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext._ 	

/******************************  Payload ERC *************************************************/


/********************  Payload Records **********************************************/


case class MachineDates_Payload ( Machine_Ser : String , StartDate : java.sql.Date , EndDate : java.sql.Date , DateValid : java.sql.Date )

case class Payload_payload(SERIAL_NUMBER : String , SMU : Option[Double] , FUELSUSEDINCYCLE : Option[Double] , VIMSTIME : Option[java.sql.Timestamp],Date : Option[ java.sql.Date ] ,LOADTRAVELDIST : Option[Double] , PAYLOADWEIGHT : Option[Double] , EMPTYTRAVELDIST : Option[Double] , LOADSTOPTIME : Option[Double] , EMPTYSTOPTIME : Option[Double] , EMPTYTRAVELTIME : Option[Double] , LOADTRAVELTIME : Option[Double] , LOADTIME : Option[Double] ,  SHIFTCOUNT : Option[Double] , CARRYBACK : Option[Double], LOADERPASSCOUNT : Option[Double]  )



/*   **********************************88   */

case class Payload_SMU_Valid ( MachSer : String , SMU_valid : Double )

case class Machine_Payload_SMU ( MACH_SER_NO : String , TID : String , EngineReplacementSMU : Option[Double])


/************************************* */



	def generateDates(sqlStartDate : java.sql.Date , sqlEndDate : java.sql.Date  ): List[java.sql.Date ] = {
		/* val startDate = java.sql.Date.valueOf( 	startDateString )
		   val endDate = java.sql.Date.valueOf( endDateString )
	
		  val sqlStartDate = java.sql.Date.valueOf( startDateString )
	          val sqlEndDate = java.sql.Date.valueOf( endDateString ) */
		
		var dateList = List[java.sql.Date]( sqlStartDate )
		val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
		val c = java.util.Calendar.getInstance()
		c.setTime( sqlStartDate )
	   	c.add( java.util.Calendar.DATE , 1 )
	 	var dt = sdf.format(c.getTime())
	 	while( java.sql.Date.valueOf(dt)  before sqlEndDate ) {
		dateList = dateList ::: List[java.sql.Date]( java.sql.Date.valueOf(dt) )
		c.add( java.util.Calendar.DATE , 1 )
		dt = sdf.format(c.getTime())
	    } 
	    dateList ::: List[java.sql.Date](java.sql.Date.valueOf(dt) )
      }

       def dropHeader(data: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
         data.mapPartitionsWithIndex((idx, lines) => {
           if (idx == 0) {
             lines.drop(1)
           }
           lines
         })
       }
	
	def getSqlDate( str : Any ) : Option[java.sql.Date] = {
		try{ Some(java.sql.Date.valueOf( str.toString  )) }
		catch{  case p : java.lang.NullPointerException => None  
			case e : java.lang.Exception => None }		 
	}
	
	def getDouble( str : Any ) : Option[Double] = {
		try{ Some(str.toString.toDouble) }
		catch { case e : java.lang.NullPointerException => None
			case e : java.lang.Exception => None }	
	}
	
	def getSqlTimestamp( str : Any ) : Option[java.sql.Timestamp] = {
		try{ Some( java.sql.Timestamp.valueOf( str.toString )  ) }
		catch{ case e : java.lang.Exception => None}
	}


val payloadFile    = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/Payload/InputFiles/Payload.csv")
		val payloadHeader  = payloadFile.take(1)(0).split(",")	
		val payloadFiltered = dropHeader(payloadFile)

		val machineListFile = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/Payload/InputFiles/MachineList.csv")
		val machineHeader   =	machineListFile.take(1)(0).split(",")	
		val indexOfMachine = machineHeader.indexOf("MACH_SER_NO")
		val indexOfStartDate = machineHeader.indexOf("StartDate")
		val machineFilter = machineListFile.filter( ! _.contains("MACH_SER_NO"))
		
		val machineDatesGenTable =  machineFilter.map( x => {
				val p = x.split(",")
				val endDate = new java.sql.Date( java.util.Calendar.getInstance().getTime().getTime() )
				
( ( p( indexOfMachine ) , java.sql.Date.valueOf(p(indexOfStartDate) ), endDate ) , generateDates( java.sql.Date.valueOf(p(indexOfStartDate)) , endDate ) )
				 } ).
			flatMapValues( x => x ).
			map( x => MachineDates_Payload ( x._1._1 , x._1._2 , x._1._3 , x._2 ) )

		
		
		val indexOfSERIAL_NUMBER =  payloadHeader.indexOf("SERIAL_NUMBER")
		val indexOfSMU =  payloadHeader.indexOf("SMU")		
		val indexOfFUELSUSEDINCYCLE =  payloadHeader.indexOf("FUELSUSEDINCYCLE")
		val indexOfDate = payloadHeader.indexOf("Date")
		val indexOfVIMSTIME = payloadHeader.indexOf("VIMSTIME")		
		val indexOfLOADTRAVELDIST  = payloadHeader.indexOf("LOADTRAVELDIST")
		val indexOfPAYLOADWEIGHT  = payloadHeader.indexOf("PAYLOADWEIGHT")
		val indexOfEMPTYTRAVELDIST  = payloadHeader.indexOf("EMPTYTRAVELDIST")
		val indexOfLOADSTOPTIME  = payloadHeader.indexOf("LOADSTOPTIME")
		val indexOfEMPTYSTOPTIME  = payloadHeader.indexOf("EMPTYSTOPTIME")
		val indexOfEMPTYTRAVELTIME  = payloadHeader.indexOf("EMPTYTRAVELTIME")
		val indexOfLOADTRAVELTIME  = payloadHeader.indexOf("LOADTRAVELTIME")
		val indexOfLOADTIME  = payloadHeader.indexOf("LOADTIME")
		val indexOfSHIFTCOUNT= payloadHeader.indexOf("SHIFTCOUNT")
		val indexOfCARRYBACK  = payloadHeader.indexOf("CARRYBACK")
		val indexOfLOADERPASSCOUNT = payloadHeader.indexOf("LOADERPASSCOUNT")

		val payloadData = payloadFiltered.map( x => { 
					val w = x.split(",")
			Payload_payload( 
				w(indexOfSERIAL_NUMBER), 
				getDouble( w(indexOfSMU) ),				
				getDouble( w( indexOfFUELSUSEDINCYCLE ) ) ,
				getSqlTimestamp( w(indexOfVIMSTIME) ),
				getSqlDate( w(indexOfDate ) ) , 
				getDouble( w(indexOfLOADTRAVELDIST) ) , 
				getDouble( w(indexOfPAYLOADWEIGHT) ) ,
				getDouble( w(indexOfEMPTYTRAVELDIST) ) ,
				getDouble( w(indexOfLOADSTOPTIME) ) ,
				getDouble( w(indexOfEMPTYSTOPTIME) ) ,
				getDouble( w(indexOfEMPTYTRAVELTIME) ) , 
				getDouble( w(indexOfLOADTRAVELTIME) ) ,  
				getDouble( w(indexOfLOADTIME) ) ,  
				getDouble( w(indexOfSHIFTCOUNT) ) , 
				getDouble( w(indexOfCARRYBACK) ) ,
				getDouble( w( indexOfLOADERPASSCOUNT))
			     )
			} )

		machineDatesGenTable.registerTempTable("machineDatesGenTable")
		payloadData.registerTempTable("payloadDataTable")
		
		val payloadTable = sqlContext.sql("SELECT  Machine_Ser ,StartDate  , EndDate , DateValid , SMU , FUELSUSEDINCYCLE , VIMSTIME , Date , LOADTRAVELDIST , PAYLOADWEIGHT , EMPTYTRAVELDIST , LOADSTOPTIME , EMPTYSTOPTIME , EMPTYTRAVELTIME , LOADTRAVELTIME , LOADTIME , SHIFTCOUNT , CARRYBACK , LOADERPASSCOUNT ,  FUELSUSEDINCYCLE/((LOADTRAVELDIST*(390+PAYLOADWEIGHT))+(EMPTYTRAVELDIST*390)) AS FCR_Ltrs_per_Kms_Tonnes , (FUELSUSEDINCYCLE*60)/(((LOADSTOPTIME+EMPTYSTOPTIME)*195)+(EMPTYTRAVELTIME*390)+(LOADTRAVELTIME*(390+PAYLOADWEIGHT))) AS FCR_LtrsperMins_Tonnes , IF(  ((PAYLOADWEIGHT >= 1.1*226.8) AND (PAYLOADWEIGHT < 1.2*226.8)) , 1 , 0  ) AS Overload_count_1_1 , IF ( PAYLOADWEIGHT >= 1.1*226.8 , 1 , 0) AS Overload_1_1 , IF ( PAYLOADWEIGHT >= 1.2*226.8 , 1 , 0 ) AS Overload_1_2_count , IF ( PAYLOADWEIGHT >= 1.2*226.8 , 1 , 0 ) AS Overload_1_2 , ((LOADTIME + EMPTYSTOPTIME + EMPTYTRAVELTIME + LOADSTOPTIME + LOADTRAVELTIME )/60 ) AS TripTime , ( PAYLOADWEIGHT * (LOADTRAVELDIST/LOADTRAVELTIME) )  AS Pyld_LdTrvSpd ,    ( ( FUELSUSEDINCYCLE )/ ((LOADTIME + EMPTYSTOPTIME + EMPTYTRAVELTIME + LOADSTOPTIME + LOADTRAVELTIME )/60 )) As FCR_LTRS_MINS  FROM machineDatesGenTable LEFT OUTER JOIN payloadDataTable ON Machine_Ser = SERIAL_NUMBER   AND DateValid = Date    ORDER BY Machine_Ser ,StartDate, DateValid , VIMSTIME ")
	
		payloadTable.registerTempTable("PayloadData")

		val payloadSummary = sqlContext.sql("SELECT  Machine_Ser ,StartDate  , EndDate , DateValid  , max( SMU ) AS SMU_payload, count( PAYLOADWEIGHT ) As No_of_Trips , avg(PAYLOADWEIGHT) As Avg_PAYLOADWEIGHT, sum(PAYLOADWEIGHT) As Sum_PAYLOADWEIGHT, sum(LOADTIME) AS Sum_LOADTIME , sum(EMPTYSTOPTIME) AS sum_EMTYSTOPTIME , sum(EMPTYTRAVELTIME) AS Sum_EMPTYTRAVELTIME , sum(EMPTYTRAVELDIST) AS Sum_EMPTYTRAVELDIST , sum(LOADSTOPTIME) As Sum_LOADSTOPTIME , sum(LOADTRAVELTIME) AS Sum_LOADTRAVELTIME , sum(LOADTRAVELDIST) AS Sum_LOADTRAVELDIST , sum(LOADERPASSCOUNT) AS Sum_LOADERPASSCOUNT , sum( FUELSUSEDINCYCLE) AS Sum_FUELSUSEDINCYCLE , sum(SHIFTCOUNT) AS Sum_SHIFTCOUNT , sum(CARRYBACK) AS Sum_CARRYBACK , sum(FCR_Ltrs_per_Kms_Tonnes) AS Sum_FCR_Ltrs_per_Kms_Tonnes, sum(FCR_LtrsperMins_Tonnes) AS Sum_FCR_LtrsperMins_Tonnes,  sum( Overload_count_1_1 ) AS Sum_Overload_count_1_1 , sum(Overload_1_1) AS sum_Overload_1_1 , sum(Overload_1_2_count) AS Sum_Overload_1_2_count , sum(Overload_1_2 ) AS Sum_Overload_1_2 , sum( TripTime ) AS Sum_TripTime , sum(Pyld_LdTrvSpd) AS Sum_Pyld_LdTrvSpd , sum( FCR_LTRS_MINS ) AS Sum_FCR_LTRS_MINS , avg(FCR_Ltrs_per_Kms_Tonnes) AS Avg_FCR_Ltrs_per_Kms_Tonnes , avg(FCR_LtrsperMins_Tonnes ) AS Avg_FCR_LtrsperMins_Tonnes , avg( FCR_LTRS_MINS ) AS Avg_FCR_LTRS_MINS  FROM PayloadData GROUP BY  Machine_Ser ,StartDate  , EndDate , DateValid  ORDER BY Machine_Ser ,StartDate, DateValid ")


		payloadSummary.registerTempTable("PayloadSummary1")
		
		val payloadSummary_formaula = sqlContext.sql("SELECT Machine_Ser ,StartDate  , EndDate , DateValid  , SMU_payload , No_of_Trips ,Avg_PAYLOADWEIGHT,Sum_PAYLOADWEIGHT, Sum_LOADTIME ,sum_EMTYSTOPTIME , Sum_EMPTYTRAVELTIME ,Sum_EMPTYTRAVELDIST , Sum_LOADSTOPTIME , Sum_LOADTRAVELTIME ,Sum_LOADTRAVELDIST ,Sum_LOADSTOPTIME ,Sum_LOADTRAVELTIME , Sum_LOADTRAVELDIST ,Sum_LOADERPASSCOUNT ,  Sum_FUELSUSEDINCYCLE , Sum_SHIFTCOUNT ,  Sum_CARRYBACK , Sum_FCR_Ltrs_per_Kms_Tonnes,  Sum_FCR_LtrsperMins_Tonnes, Sum_Overload_count_1_1 , sum_Overload_1_1 ,Sum_Overload_1_2_count , Sum_Overload_1_2 , Sum_TripTime , Sum_Pyld_LdTrvSpd , Sum_FCR_LTRS_MINS , Avg_FCR_Ltrs_per_Kms_Tonnes , Avg_FCR_LtrsperMins_Tonnes , Avg_FCR_LTRS_MINS , (Sum_FUELSUSEDINCYCLE / No_of_Trips ) AS Avg_Ltrs_per_trips , IF( SMU_payload is NULL , 0, 1 ) AS payload_dataDummy  FROM PayloadSummary1 ORDER BY Machine_Ser ,StartDate,EndDate, DateValid ")

/*********************************************Post Payload*********************************************************************/

def generatePayloadBins(  max : Any ) : List[Double] = {
	
		val maxSMU = max.toString.toDouble
		var list: List[Double]= List[Double](  )
		var temp = 100.0
		while(temp < maxSMU){
			
			list = list ::: List(temp )	
			temp = temp + 100.0	
		}
				
		list
}

payloadSummary_formaula.registerTempTable("payloadSummary_formaula")
		
		val filteredSmuPayload = sqlContext.sql( "SELECT * FROM payloadSummary_formaula WHERE SMU_payload < 40000  ")
		filteredSmuPayload.registerTempTable("filteredSmuPayload")
		
		val filteredSmuPayloadSummary = sqlContext.sql( "SELECT Machine_Ser , 100 AS min_SMU_payload , max(SMU_payload) AS max_SMU_payload FROM filteredSmuPayload GROUP BY Machine_Ser ")
		
		val payloadBins = filteredSmuPayloadSummary.map(x => (x(0).toString , generatePayloadBins(x(2)) )  ).flatMapValues( x => x ).map( x => Payload_SMU_Valid( x._1 , x._2) )
		
		
		val machineDataFile = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/Payload/InputFiles/AfterPayloadMachineTable.csv")

		val indexOfMachine_ser_no = machineDataFile.take(1)(0).split(","). indexOf("MACH_SER_NO")
		val indexOfTID = machineDataFile.take(1)(0).split(","). indexOf("Westrac ID")
		val indexOfEngineLife = machineDataFile.take(1)(0).split(","). indexOf("Life Used when Engine Changeout")
		
		val filteredmachineDataFile = dropHeader(machineDataFile ) .
						map( x => 
					Machine_Payload_SMU ( 
							x.split(",")(indexOfMachine_ser_no) , 
							x.split(",")(indexOfTID) , 
							try {
				Some( ( Math.floor( x.split(",")(indexOfEngineLife).toDouble / 100 )  * 100)  + 100    ) 
							} catch { case e : java.lang.Exception => None}  
							)
 						)

		filteredmachineDataFile.registerTempTable("filteredmachineDataFile")
		
		payloadBins.registerTempTable("payloadBins")

		println( payloadBins.count )

		val payloadBinsJoin1 = sqlContext.sql("SELECT MachSer , SMU_valid , TID , EngineReplacementSMU  FROM payloadBins LEFT OUTER JOIN  filteredmachineDataFile ON MachSer =  MACH_SER_NO ")

		payloadBinsJoin1.registerTempTable("payloadBinsJoin1")

		

		def smuBinFloor ( x : Any ) : Option[Double] = {
				try{			
				Some(  ( Math.floor( x.toString.toDouble / 100) * 100 ) + 100 )
				 }
				catch {
					case p : java.lang.NullPointerException => None
					case e : java.lang.Exception => None
				}
		}
		
		sqlContext.registerFunction("smuBinFloor", smuBinFloor( _ : Any) )
		
		payloadSummary_formaula.registerTempTable("payloadSummary_formaula") 
		
		val payload_formula2 = sqlContext.sql("SELECT Machine_Ser ,StartDate  , EndDate , DateValid  , SMU_payload , No_of_Trips ,Avg_PAYLOADWEIGHT,Sum_PAYLOADWEIGHT, Sum_LOADTIME ,sum_EMTYSTOPTIME , Sum_EMPTYTRAVELTIME ,Sum_EMPTYTRAVELDIST , Sum_LOADSTOPTIME , Sum_LOADTRAVELTIME ,Sum_LOADTRAVELDIST ,Sum_LOADSTOPTIME ,Sum_LOADTRAVELTIME , Sum_LOADTRAVELDIST ,Sum_LOADERPASSCOUNT ,  Sum_FUELSUSEDINCYCLE , Sum_SHIFTCOUNT ,  Sum_CARRYBACK , Sum_FCR_Ltrs_per_Kms_Tonnes,  Sum_FCR_LtrsperMins_Tonnes, Sum_Overload_count_1_1 , sum_Overload_1_1 ,Sum_Overload_1_2_count , Sum_Overload_1_2 , Sum_TripTime , Sum_Pyld_LdTrvSpd , Sum_FCR_LTRS_MINS , Avg_FCR_Ltrs_per_Kms_Tonnes , Avg_FCR_LtrsperMins_Tonnes , Avg_FCR_LTRS_MINS , smuBinFloor(SMU_payload) AS SMU_payload1  FROM payloadSummary_formaula ORDER BY Machine_Ser ,StartDate,EndDate, DateValid ")

		payload_formula2.registerTempTable("payload_formula2")

		val payload_join2 = sqlContext.sql("SELECT MachSer , SMU_valid AS Bin , TID , EngineReplacementSMU , StartDate  , EndDate , DateValid  , SMU_payload , No_of_Trips ,Avg_PAYLOADWEIGHT,Sum_PAYLOADWEIGHT, Sum_LOADTIME ,sum_EMTYSTOPTIME , Sum_EMPTYTRAVELTIME ,Sum_EMPTYTRAVELDIST , Sum_LOADSTOPTIME , Sum_LOADTRAVELTIME ,Sum_LOADTRAVELDIST ,Sum_LOADSTOPTIME ,Sum_LOADTRAVELTIME , Sum_LOADTRAVELDIST ,Sum_LOADERPASSCOUNT ,  Sum_FUELSUSEDINCYCLE , Sum_SHIFTCOUNT ,  Sum_CARRYBACK , Sum_FCR_Ltrs_per_Kms_Tonnes,  Sum_FCR_LtrsperMins_Tonnes, Sum_Overload_count_1_1 , sum_Overload_1_1 ,Sum_Overload_1_2_count , Sum_Overload_1_2 , Sum_TripTime , Sum_Pyld_LdTrvSpd , Sum_FCR_LTRS_MINS , Avg_FCR_Ltrs_per_Kms_Tonnes , Avg_FCR_LtrsperMins_Tonnes , Avg_FCR_LTRS_MINS , SMU_payload1  FROM payloadBinsJoin1 LEFT OUTER JOIN payload_formula2 ON MachSer = Machine_Ser AND SMU_valid = SMU_payload1   ")

	payload_join2.registerTempTable("payload_join2")

		val payload_Result_Variables = sqlContext.sql("SELECT MachSer , TID , Bin , min(DateValid) AS minDate , max(DateValid) AS maxDate , min(SMU_payload) AS min_SMU , max(SMU_payload) AS max_payload , avg(Sum_PAYLOADWEIGHT) AS Avg_Sum_PAYLOADWEIGHT , avg(Sum_SHIFTCOUNT) AS Avg_Sum_SHIFTCOUNT , avg( Sum_FCR_Ltrs_per_Kms_Tonnes ) AS Avg_Sum_FCR_Ltrs_per_Kms_Tonnes , avg(Sum_FCR_LtrsperMins_Tonnes ) AS Avg_Sum_FCR_LtrsperMins_Tonnes , sum(Sum_Overload_count_1_1 ) AS Sum_Sum_Overload_count_1_1 , avg(sum_Overload_1_1) AS Avg_sum_Overload_1_1 , avg(Sum_TripTime) AS Avg_Sum_TripTime , avg(Sum_Pyld_LdTrvSpd) AS Avg_Sum_Pyld_LdTrvSpd , avg(Sum_FCR_LTRS_MINS) AS Avg_Sum_FCR_LTRS_MINS , avg(No_of_Trips) AS Avg_No_of_Trips FROM payload_join2 GROUP BY MachSer , TID , Bin , EngineReplacementSMU  ORDER BY MachSer , TID , Bin ")

               payload_Result_Variables.registerTempTable("payload_Result_Variables")

/*************************************   Exception ERC *****************************************************************************/

  /*************************  EXception Records *************************************************/

case class ExceptionERC (Severity : String , Data_Element : String , Date : java.sql.Date , Serial_Number : String, SMU : Double, Critical_Fluid: Double,  Critical_Event:Double, Critical_Trend: Double, Critical_Datalogger :Double, Critical_All: Double, Event:Double, Trend:Double, Fluid:Double, Datalogger:Double, Critical_Fluid2: Double,  Critical_Event2:Double, Critical_Trend2: Double, Critical_Datalogger2 :Double, Critical_All2: Double )

case class MasterTable( machineNo : String , startDate : java.sql.Date , dateValid : java.sql.Date ) 


/*********************************************** Exception Code ****************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

object ExceptionERC {

/* var used for generating indexs Of headers in input file */
var indexOfSeverity : Int = -1
var indexOfDataelement : Int = -1
var indexOfOccurence : Int = -1
var indexOfSerial : Int = -1



var indexOfMachNo : Int = -1
var indexOfStartDate : Int = -1
var indexOfEndDate : Int = -1

/* function to transform string into dates */
def repairDate( str : String ) : java.sql.Date = { 
 	val pieces = str.split(" ")(0).split("/")
	val dateString  = (pieces(2)+"-"+pieces(0)+"-"+pieces(1))
	val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
        new java.sql.Date (format.parse( dateString ).getTime() )
}

/* function to format severity field */
def severityFormat(str: String): String = {
	val pieces = str.split(" ")
	val format = pieces(1)
	format
	}

/* function to calcuate value of critical fluids */
def Critical_Fluid(line : String ) : Double = { 
         val words = line.split(",")
         val Fluid_Critical = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ) )1  else  0 }  
         val Fluid_Severe = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) ) 1 else  0 }
         Fluid_Critical +  Fluid_Severe
}


/* function to calcuate value of critical events */
def Critical_Event ( line : String) : Double = { 
    val words = line.split(",")
    val Event_Critical = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ) ) 1  else  0 }
    val Event_Severe = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) ) 1 else  0  }
    Event_Critical + Event_Severe 
}


/* function to calcuate value of critical trends */
def Critical_Trend ( line : String ) : Double = { 
   val words = line.split(",")
   val Trend_Critical = {  if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ) ) 1 else  0  }
   val Trend_Severe = {  if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) ) 1  else 0  }
   Trend_Critical + Trend_Severe
}

 
/* function to calcuate value of critical datalogger data */
def Critical_Datalogger( line : String ) : Double = { 
    val words = line.split(",")
    val Datalogger_Critical = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ))  1  else   0 }
    val Datalogger_Severe = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) )1 else  0 }
    Datalogger_Critical + Datalogger_Severe
}

/* function to calculate all critical value */
def Critical_All( line : String ) : Double = {
    Critical_Fluid(line) + Critical_Event( line ) + Critical_Trend ( line ) + Critical_Datalogger ( line ) 
 
}


def Critical_Fluid2(line : String ) : Double = { 
         val words = line.split(",")
         val Fluid_Critical2 = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ) )1  else  0 }  
         val Fluid_Severe2 = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) ) 1 else  0 }
		 val Fluid_Major2 = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Major") ) )1  else  0 }  
         Fluid_Critical2 +  Fluid_Severe2 + Fluid_Major2
}


/* function to calcuate value of critical events */
def Critical_Event2 ( line : String) : Double = { 
    val words = line.split(",")
    val Event_Critical2 = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ) ) 1  else  0 }
    val Event_Severe2 = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) ) 1 else  0  }
	val Event_Major2 = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Major") ) ) 1 else  0  }
    Event_Critical2 + Event_Severe2 + Event_Major2
}


/* function to calcuate value of critical trends */
def Critical_Trend2 ( line : String ) : Double = { 
   val words = line.split(",")
   val Trend_Critical2 = {  if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ) ) 1 else  0  }
   val Trend_Severe2 = {  if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) ) 1  else 0  }
   val Trend_Major2 = {  if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Major") ) ) 1  else 0  }
   Trend_Critical2 + Trend_Severe2 + Trend_Major2
}

 
/* function to calcuate value of critical datalogger data */
def Critical_Datalogger2( line : String ) : Double = { 
    val words = line.split(",")
    val Datalogger_Critical2 = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ))  1  else   0 }
    val Datalogger_Severe2 = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) )1 else  0 }
	val Datalogger_Major2 = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Major") ) )1 else  0 }

    Datalogger_Critical2 + Datalogger_Severe2 + Datalogger_Major2
}

/* function to calculate all critical value */
def Critical_All2( line : String ) : Double = {
    Critical_Fluid2(line) + Critical_Event2( line ) + Critical_Trend2 ( line ) + Critical_Datalogger2 ( line ) 
 
}






/* function to calculate severity of events */
def Event ( line : String ) : Double = {
  val words = line.split(",")
  val Event_Critical = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ) )1 else    0 }
  val Event_Severe   = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Severe") )  ) 1   else   0 }
  val Event_Major  = {  if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Major") )  )  1  else    0   }
  val Event_Moderate = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Moderate") ) )  1  else   0  }
  val Event_Minor  = { if( words(indexOfDataelement).equals("EVENT") && ( severityFormat(words(indexOfSeverity)).equals("Minor") ) ) 1  else   0  }

      Event_Critical + Event_Severe + Event_Major + Event_Moderate + Event_Minor 

}


 
/* function to calculate severity of trends */
def Trend ( line : String ) : Double = {
  val words = line.split(",") 
  
  val Trend_Critical = {   if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Critical") )  )   1   else     0  }
  val Trend_Severe = {    if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) ) 1 else     0   }
  val Trend_Major = {  if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Major") )  ) 1   else    0    }
  val Trend_Moderate = {  if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Moderate") ) )  1   else    0 }
  val Trend_Minor = {   if( words(indexOfDataelement).equals("TREND") && ( severityFormat(words(indexOfSeverity)).equals("Minor") ) ) 1  else   0   }
        Trend_Critical +Trend_Severe + Trend_Major + Trend_Moderate + Trend_Minor 
 
}

/* function to calculate severity of fluid */
def Fluid( line : String ) : Double = {
   val words = line.split(",")  
   val Fluid_Critical = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Critical") )  )  1  else   0  }
   val Fluid_Severe = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Severe") )  ) 1   else  0  }
   val Fluid_Major = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Major") ) ) 1   else  0  }
   val Fluid_Moderate = {if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Moderate") ) )1  else    0  }
   val Fluid_Minor = { if( words(indexOfDataelement).equals("FLUID") && ( severityFormat(words(indexOfSeverity)).equals("Minor") ) ) 1   else   0  }

           Fluid_Critical + Fluid_Severe + Fluid_Major + Fluid_Moderate + Fluid_Minor 
     
}


 
/* function to calculate severity of datalogger data */
def Datalogger ( line : String ) : Double = {
    val words = line.split(",")
    
    val Datalogger_Critical = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Critical") ) ) 1   else   0 }
    val Datalogger_Severe  = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Severe") ) )   1   else  0    } 
    val Datalogger_Major = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Major") )  ) 1  else   0   }
    val Datalogger_Moderate = {  if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Moderate") ) )1  else   0 }
    val   Datalogger_Minor = { if( words(indexOfDataelement).equals("DATALOGGER") && ( severityFormat(words(indexOfSeverity)).equals("Minor") ) ) 1   else  0 }
    Datalogger_Critical + Datalogger_Severe + Datalogger_Major + Datalogger_Moderate + Datalogger_Minor 
    
}






/************************************************************************************************************************/
/* function to evalute Exception data */

def evaluteException(  sc : SparkContext , filepath : Array[String] ) : org.apache.spark.rdd.RDD[String] = {


/* create new sql context to use sql queries and import required packages
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	     import sqlContext._
	    import sqlContext.createSchemaRDD
*/

/* read exception input Data and take header row */

val exceptionIndex = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/exception24-11/ExceptionInputData.csv").take(1)(0).split(",")

/* generate index of headers from input file */
 indexOfSeverity = exceptionIndex.indexOf("Severity")
 indexOfDataelement = exceptionIndex.indexOf("Data Element")
 indexOfOccurence = exceptionIndex.indexOf("Date of Occurrence")
 indexOfSerial = exceptionIndex.indexOf("Serial Number")

/* read exception input Data and filter out header row */
val exceptionData = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/exception24-11/ExceptionInputData.csv").filter(! _.contains("Severity") ).distinct

/* create schema for exception Data using "Exception" case class */
val filteredException = exceptionData.map( line => { val words = line.split(",");  ExceptionERC( severityFormat(words(indexOfSeverity)), words(indexOfDataelement), repairDate(words(indexOfOccurence)), words(indexOfSerial), 0 ,Critical_Fluid(line), Critical_Event(line), Critical_Trend(line), Critical_Datalogger(line), Critical_All(line),  Event(line), Trend(line), Fluid(line), Datalogger(line), Critical_Fluid2(line), Critical_Event2(line), Critical_Trend2(line), Critical_Datalogger2(line), Critical_All2(line)   ) } )


filteredException.registerTempTable("ExceptionTable")

val aggregatedException = sqlContext.sql( "SELECT sum(Critical_Fluid) as Critical_Fluid, sum(Critical_Event) as Critical_Event, sum(Critical_Trend) as Critical_Trend, sum(Critical_Datalogger) as Critical_Datalogger, sum(Critical_All) as Critical_All, sum(Fluid) as Fluid, sum(Event) as Event, sum(Trend) as Trend, sum(Datalogger) as Datalogger, Date, Serial_Number, sum(Critical_Fluid2) as Critical_Fluid2, sum(Critical_Event2) as Critical_Event2, sum(Critical_Trend2) as Critical_Trend2, sum(Critical_Datalogger2) as Critical_Datalogger2, sum(Critical_All2) as Critical_All2 FROM ExceptionTable GROUP BY Serial_Number, Date, SMU" )

/* read machine List input Data and take header row */
val machineIndex = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/exception24-11/machineList.csv").take(1)(0).split(",")
 indexOfMachNo = machineIndex.indexOf("MACH_SER_NO")
 indexOfStartDate = machineIndex.indexOf("StartDate")
 indexOfEndDate = machineIndex.indexOf("EndDate")



/* read machine List input Data and filter out header row */
val rawMachineList = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/exception24-11/machineList.csv").filter(!_.contains("StartDate"))

/* fillup dates from start Date to end data for each machine */
val  machineRDD = rawMachineList.map( x => { val w = x.split(",")
			  
			  val sqlStartDate = java.sql.Date.valueOf( w(indexOfStartDate).toString )
			  val sqlEndDate = java.sql.Date.valueOf( w(indexOfEndDate).toString )
			  var dateList = List[(java.sql.Date , java.sql.Date)]( (sqlStartDate, sqlStartDate ) )
			  			   
			  val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
			  val c = java.util.Calendar.getInstance()
			  c.setTime( sdf.parse(w(indexOfStartDate).toString) )
			  c.add( java.util.Calendar.DATE , 1 )
			  var dt = sdf.format(c.getTime())
			  while( java.sql.Date.valueOf(dt)  before sqlEndDate ) {
				
				 dateList = dateList ::: List[( java.sql.Date , java.sql.Date )]( (java.sql.Date.valueOf(dt) , sqlStartDate ))
				 c.add( java.util.Calendar.DATE , 1 )
				 dt = sdf.format(c.getTime())
			  } 
			  (  w(indexOfMachNo) ,  dateList ::: List[(java.sql.Date , java.sql.Date )] (  ( java.sql.Date.valueOf(dt) , sqlStartDate ) ) )

		        } ) 

val masterFlatRDD = new PairRDDFunctions(machineRDD).flatMapValues( x => x ) 

     println ( masterFlatRDD.count )


val masterMachineRDD = masterFlatRDD.map( x =>  MasterTable ( x._1 , x._2._2 , x._2._1 ) ) 



aggregatedException.registerTempTable("aggregatedExceptionTable")
masterMachineRDD.registerTempTable("masterMachineRDDTable")

val leftJoined = sqlContext.sql("SELECT machineNo ,startDate, dateValid ,Critical_Fluid ,Critical_Event , Critical_Trend , Critical_Datalogger , Critical_All , Fluid , Event , Trend , Datalogger , Date , Critical_Fluid2 , Critical_Event2 , Critical_Trend2 ,  Critical_Datalogger2 , Critical_All2     FROM masterMachineRDDTable LEFT JOIN aggregatedExceptionTable ON  masterMachineRDDTable.machineNo = aggregatedExceptionTable.Serial_Number  AND masterMachineRDDTable.dateValid = aggregatedExceptionTable.Date ")

leftJoined.registerTempTable("leftJoinedTable")


val exceptionOutput = sqlContext.sql("SELECT * FROM leftJoinedTable ORDER BY  machineNo, startDate, dateValid")


/* transform output data into string format */
val finalExceptionOutput = exceptionOutput.map(x => x.mkString(","))

finalExceptionOutput






/********************************************  Post Exception ERC *****************************************************************/

/*********************************************************************************************************************************/



leftJoined.registerTempTable("ExceptionResult")


payloadSummary_formaula.registerTempTable("payloadSummary_formaula")

def exception_Bin( x : Any ) : Option[Double] = {
	try{  Some( ( Math.floor( x.toString.toDouble/100 ) * 100 ) + 100  ) }
	catch{ 
		case e : java.lang.NullPointerException  => None 		
		case e : java.lang.Exception => None }
}

sqlContext.registerFunction( "exception_Bin" , exception_Bin( _ : Any ) )

val ExceptionJoinedPayload = sqlContext.sql( "SELECT  machineNo ,startDate, dateValid ,Critical_Fluid ,Critical_Event , Critical_Trend , Critical_Datalogger , Critical_All , Fluid , Event , Trend , Datalogger , Date , Critical_Fluid2 , Critical_Event2 , Critical_Trend2 ,  Critical_Datalogger2 , Critical_All2 , exception_Bin( SMU_payload ) As SMU_Exception , SMU_payload FROM ExceptionResult JOIN payloadSummary_formaula ON  machineNo =  Machine_Ser AND dateValid = DateValid " )

ExceptionJoinedPayload.registerTempTable("ExceptionJoinedPayload")

ExceptionJoinedPayload.cache()

val ExceptionJoinsBins = sqlContext.sql("SELECT MachSer , SMU_valid , TID , EngineReplacementSMU , startDate, dateValid ,Critical_Fluid ,Critical_Event , Critical_Trend , Critical_Datalogger , Critical_All , Fluid , Event , Trend , Datalogger , Critical_Fluid2 , Critical_Event2 , Critical_Trend2 ,  Critical_Datalogger2 , Critical_All2 , SMU_Exception , SMU_payload  FROM payloadBinsJoin1 LEFT OUTER JOIN ExceptionJoinedPayload ON MachSer = machineNo AND SMU_valid = SMU_Exception ")


ExceptionJoinsBins.registerTempTable("ExceptionJoinsBins")

val ExceptionSummary = sqlContext.sql("SELECT MachSer , SMU_valid , TID , EngineReplacementSMU , sum( Critical_Fluid ) As Sum_Critical_Fluid , sum(Critical_Event) As Sum_Critical_Event , sum(Critical_Trend) As Sum_Critical_Trend , sum(Critical_Datalogger) As Sum_Critical_Datalogger , sum(Critical_All) AS Sum_Critical_All , Sum(Event ) As Sum_Event , sum(Fluid) As Sum_Fluid , sum( Trend ) As Sum_Trend , sum(Datalogger) As Sum_Datalogger , min(dateValid) As Min_Date_exception , max( dateValid ) AS Max_Date_exception  , min(SMU_payload) AS min_SMU_payload_Exception , max(SMU_payload) AS max_SMU_payload_Exception    FROM  ExceptionJoinsBins GROUP BY MachSer , SMU_valid , TID , EngineReplacementSMU  ")


ExceptionSummary.registerTempTable("ExceptionSummary")


def dayDiff_Exception ( day1 : Any , day2 : Any ) : Option[Double] = {
		
	try { Some( (java.sql.Date.valueOf( day1.toString ).getTime - java.sql.Date.valueOf( day1.toString ).getTime ).toDouble / 24 * 60 * 60 * 1000  ) }
	catch {
		case e : java.lang.NullPointerException => None 
		case e : java.lang.Exception => None
	}	
}



sqlContext.registerFunction("dayDiff_Exception" , dayDiff_Exception( _ : Any , _ : Any ) )

val ExceptionERCVariables = sqlContext.sql("SELECT MachSer AS Mach_E , SMU_valid AS SMU_E  , TID AS TID_E ,  (Sum_Critical_Trend / Sum_Trend) As CriticalPercent_Trend , ( Sum_Critical_Event / Sum_Event   ) AS CriticalPercent_Event , ( Sum_Critical_Fluid / Sum_Fluid  ) As CriticalPercent_Fluid , (Sum_Critical_Datalogger / Sum_Datalogger ) As CriticalPercent_Datalogger , ( Sum_Critical_All / ( max_SMU_payload_Exception - min_SMU_payload_Exception ) )  AS CriticalHours , ( Sum_Critical_All / ( dayDiff_Exception( Max_Date_exception , Min_Date_exception ) ) ) AS CriticalDays FROM ExceptionSummary  ")


ExceptionERCVariables.registerTempTable( "ExceptionERCVariables" )


val Payload_Exception_Variables = sqlContext.sql(" SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload ,  Avg_Sum_PAYLOADWEIGHT ,  Avg_Sum_SHIFTCOUNT ,  Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_sum_Overload_1_1 , Avg_Sum_TripTime ,  Avg_Sum_Pyld_LdTrvSpd ,  Avg_Sum_FCR_LTRS_MINS ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalPercent_Datalogger ,CriticalHours , CriticalDays FROM payload_Result_Variables LEFT OUTER JOIN ExceptionERCVariables ON   MachSer = Mach_E AND SMU_E = Bin  AND TID = TID_E ")

  Payload_Exception_Variables.registerTempTable("Payload_Exception_Variables")


/***************************************************************************************************************************************
		  Events ERC 
			
******************************************************************************************************************************************/

 /**************************************  Events ERC Records ***************************************************************************/ 

//case class Exception (Severity : String , Data_Element : String , Date : java.sql.Date , Serial_Number : String, SMU : Double, Critical_Fluid: Double,  Critical_Event:Double, Critical_Trend: Double, Critical_Datalogger :Double, Critical_All: Double, Event:Double, Trend:Double, Fluid:Double, Datalogger:Double, Critical_Fluid2: Double,  Critical_Event2:Double, Critical_Trend2: Double, Critical_Datalogger2 :Double, Critical_All2: Double )

case class MasterTable( machineNo : String , startDate : java.sql.Date , dateValid : java.sql.Date ) 


/*******************************************************************************************************************/

/************************************************  Events ERC    *********************************************************************/



object EventERC {

var indexOfSerialNo:Int = -1
var indexOfVIMSTIME:Int = -1
var indexOfSMU:Int = -1
var indexOfEVENT_ID:Int = -1
var indexOfSOURCE:Int = -1
var indexOfChannel_Id:Int = -1
var indexOfDescription:Int = -1
var indexOfAct_Lmt:Int = -1
var indexOfAck_Time:Int = -1
var indexOfAck_No:Int = -1
var indexOfDuration:Int = -1
var indexOfFMI:Int = -1
var indexOfMID:Int = -1
var indexOfCID:Int = -1


var indexOfMach_Ser_No:Int = -1
var indexOfStartDate:Int = -1
var indexOfEndDate:Int = -1


var indexOfAbuseEventId:Int = -1
var indexOfAbuseDescription:Int = -1
var indexOfAbuseEvent:Int = -1

def repairDate( str : String ) : java.sql.Date = { 
 	val dateString = str.split(" ")(0)
	val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
	new java.sql.Date (format.parse( dateString ).getTime() )
}


def evaluteEvents(  sc : SparkContext , filepath : Array[String] ) : org.apache.spark.rdd.RDD[String] = {


val eventsIndex = sc.textFile("/home/amjath/Desktop/AllERC/events/eventsInputFile.csv").take(1)(0).split(",")
	 indexOfSerialNo = eventsIndex.indexOf("SERIAL_NUMBER")
	 indexOfVIMSTIME = eventsIndex.indexOf("VIMSTIME")
	 indexOfSMU = eventsIndex.indexOf("SMU")
	 indexOfEVENT_ID = eventsIndex.indexOf("EVENT_ID")
	 indexOfSOURCE = eventsIndex.indexOf("SOURCE")
	 indexOfChannel_Id = eventsIndex.indexOf("CHANNEL_ID")
	 indexOfDescription = eventsIndex.indexOf("DESCRIPTION")
	 indexOfAct_Lmt = eventsIndex.indexOf("ACT_LMT")
	 indexOfAck_Time = eventsIndex.indexOf("ACKNOWLEDGE_TIME")
	 indexOfAck_No = eventsIndex.indexOf("ACKNOWLEDGE_NUMBER")
	 indexOfDuration = eventsIndex.indexOf("DURATION")
	 indexOfFMI = eventsIndex.indexOf("FMI")
	 indexOfMID = eventsIndex.indexOf("MID")
	 indexOfCID = eventsIndex.indexOf("CID")


val rawEvents = sc.textFile("/home/amjath/Desktop/AllERC/events/eventsInputFile.csv").filter(!_.contains("SMU"))



/* Creating Schema for events Table */

val eventSchema = StructType(Array(StructField("SerialNo",StringType,true),StructField("Date",DateType,true),StructField("SMU",StringType,true),StructField("EventID",DoubleType,true),StructField("Source",IntegerType,true),StructField("ChannelID",StringType,true),StructField("Description",StringType,true),StructField("ActLmt",StringType,true),StructField("AckTime",IntegerType,true),StructField("ActNumber",IntegerType,true),StructField("FMI",IntegerType,true),StructField("MID",IntegerType,true),StructField("CID",IntegerType,true)))



/* Mapping rows from raw events data */

val eventRowRDD = rawEvents.map(_.split(",")).map(p => Row(p(indexOfSerialNo),repairDate(p(indexOfVIMSTIME)), (p(indexOfSMU)), (p(indexOfEVENT_ID)), (p(indexOfSOURCE)), p(indexOfChannel_Id), p(indexOfDescription), p(indexOfAct_Lmt), (p(indexOfAck_Time)),     (p(indexOfAck_No)), (p(indexOfFMI)), (p(indexOfMID)), (p(indexOfCID))) )







/* applying rows to schema */

val eventSchemaRDD = sqlContext.applySchema(eventRowRDD, eventSchema)

eventSchemaRDD.registerTempTable("EventsTable")




val machineIndex = sc.textFile("/home/amjath/Desktop/AllERC/events/eventsMachinelistInput.csv").take(1)(0).split(",")

var indexOfMach_Ser_No = machineIndex.indexOf("MACH_SER_NO")
var indexOfStartDate = machineIndex.indexOf("StartDate")
var indexOfEndDate = machineIndex.indexOf("EndDate")


val rawMachineList = sc.textFile("/home/amjath/Desktop/AllERC/events/eventsMachinelistInput.csv").filter(!_.contains("StartDate"))

val  machineRDD = rawMachineList.map( x => { val w = x.split(",")
			  
			   /* println( w(1) + "    " + w(2) ) */
			  val sqlStartDate = java.sql.Date.valueOf( w(indexOfStartDate).toString )
			  val sqlEndDate = java.sql.Date.valueOf( w(indexOfEndDate).toString )
			  var dateList = List[(java.sql.Date , java.sql.Date , java.sql.Date)]( (sqlStartDate, sqlStartDate , sqlEndDate ) )
			  			   
			  val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
			  val c = java.util.Calendar.getInstance()
			  c.setTime( sdf.parse(w(indexOfStartDate).toString) )
			  c.add( java.util.Calendar.DATE , 1 )
			  var dt = sdf.format(c.getTime())
			  while( java.sql.Date.valueOf(dt)  before sqlEndDate ) {
				
			dateList = dateList ::: List[( java.sql.Date , java.sql.Date , java.sql.Date )]( (java.sql.Date.valueOf(dt) , sqlStartDate , sqlEndDate ))
			    c.add( java.util.Calendar.DATE , 1 )
			    dt = sdf.format(c.getTime())
			  } 

		 (  w(indexOfMach_Ser_No) ,  dateList ::: List[(java.sql.Date , java.sql.Date , java.sql.Date )] (  ( java.sql.Date.valueOf(dt) , sqlStartDate , sqlEndDate ) ) )

		        } ) 

val masterFlatRDD = new PairRDDFunctions(machineRDD).flatMapValues( x => x ) 



val machineSchema = StructType(Array(StructField("Machine_No",StringType,true),StructField("Start_Date",DateType,true),StructField("End_Date",DateType,true),StructField("Date_Valid",DateType,true)))




val machineRowRDD = masterFlatRDD.map( x =>  Row ( x._1 , x._2._2 , x._2._3  , x._2._1) ) 

val machineSchemaRDD = sqlContext.applySchema(machineRowRDD, machineSchema)

machineSchemaRDD.registerTempTable("MachineTable")



val machineJoinedEvents = sqlContext.sql("SELECT * FROM MachineTable LEFT JOIN EventsTable ON  MachineTable.Machine_No = EventsTable.SerialNo  AND MachineTable.Date_Valid = EventsTable.Date")

machineJoinedEvents.registerTempTable("machineJoinedEventsTable")


val orderedEvents = sqlContext.sql("SELECT * FROM machineJoinedEventsTable ORDER BY  Machine_No, Start_Date, Date_Valid")

orderedEvents.registerTempTable("orderedEventsTable")


val abuseIndex = sc.textFile("/home/amjath/Desktop/AllERC/events/abuseEventsInput.csv").take(1)(0).split(",")
	indexOfAbuseEventId = abuseIndex.indexOf("EVENT_ID")
	indexOfAbuseDescription= abuseIndex.indexOf("DESCRIPTION")
	indexOfAbuseEvent= abuseIndex.indexOf("AbuseEvent")


val rawAbuseEvents = sc.textFile("/home/amjath/Desktop/AllERC/events/abuseEventsInput.csv").filter(!_.contains("AbuseEvent"))


val abuseSchema = StructType(Array(StructField("Event_ID",DoubleType,true),StructField("Description",StringType,true),StructField("Abuse_Event",StringType,true) ))

val abuseRowRDD = rawAbuseEvents.map(_.split(",")).map(p => Row(p(indexOfAbuseEventId), p(indexOfAbuseDescription), p(indexOfAbuseEvent) ) )

val abuseSchemaRDD = sqlContext.applySchema(abuseRowRDD, abuseSchema)

abuseSchemaRDD.registerTempTable("abuseEventsTable")

val eventsJoinedAbuseEvents = sqlContext.sql("SELECT * FROM orderedEventsTable LEFT JOIN abuseEventsTable ON  orderedEventsTable.Description = abuseEventsTable.Description  AND orderedEventsTable.EventID = abuseEventsTable.Event_ID")

eventsJoinedAbuseEvents.registerTempTable("eventsJoinedAbuseEventsTable")

val aggregatedEvents = sqlContext.sql( "SELECT Machine_No as MACH_SER_NO, Start_Date as StartDate, End_Date as EndDate, Date_Valid as DateValid, Date, max(SMU) as Max_SMU, Abuse_Event,  count(Abuse_Event) as CountofAbuseEvents     FROM eventsJoinedAbuseEventsTable GROUP BY Machine_No, Start_Date, End_Date, Date_Valid, Date,  Abuse_Event ORDER BY Machine_No, Start_Date, End_Date, Date_Valid, CountofAbuseEvents" )

aggregatedEvents.registerTempTable("aggregatedEventsTable")

sqlContext.sql("SELECT * FROM aggregatedEventsTable WHERE Max_SMU IS NULL ")

def endOvrSpd(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Eng OvrSpd")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}

sqlContext.registerFunction("endOvrSpd", endOvrSpd(_ :String, _ : Any))


def engPreLubeOvr(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Engine Pre-lube Override")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}

sqlContext.registerFunction("engPreLubeOvr", engPreLubeOvr(_ :String, _ : Any))


def grdLvlShut(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("GND LVL SHTDN")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {	case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}

sqlContext.registerFunction("grdLvlShut", grdLvlShut(_ :String, _ : Any))


def brkRealted(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Brk Related")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}

sqlContext.registerFunction("brkRealted", brkRealted(_ :String, _ : Any))


def hiOilTemp(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Hi Strg Oil Tmp")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}

sqlContext.registerFunction("hiOilTemp", hiOilTemp(_ :String, _ : Any))


def hiFuelWtr(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("HiFul/Wtr SpLvl")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}

sqlContext.registerFunction("hiFuelWtr", hiFuelWtr(_ :String, _ : Any))


def idlStndRely(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Idle Stdn Rly")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}

sqlContext.registerFunction("idlStndRely", idlStndRely(_ :String, _ : Any))


def payloadOvr(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Pyld Ovrld Abuse")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}

sqlContext.registerFunction("payloadOvr", payloadOvr(_ :String, _ : Any))


val eventsCal = sqlContext.sql("SELECT MACH_SER_NO, StartDate, EndDate, DateValid, Date, Max_SMU, endOvrSpd(Abuse_Event,CountofAbuseEvents) as endOvrSpd1, engPreLubeOvr(Abuse_Event,CountofAbuseEvents) as engPreLubeOvr1, grdLvlShut(Abuse_Event,CountofAbuseEvents) as grdLvlShut1, brkRealted(Abuse_Event,CountofAbuseEvents) as brkRealted1, hiOilTemp(Abuse_Event,CountofAbuseEvents) as hiOilTemp1, hiFuelWtr(Abuse_Event,CountofAbuseEvents) as hiFuelWtr1,idlStndRely(Abuse_Event,CountofAbuseEvents) as idlStndRely1,payloadOvr(Abuse_Event,CountofAbuseEvents) as payloadOvr1 FROM aggregatedEventsTable")

eventsCal.registerTempTable("eventsCalTable")

val finalEventsOutput = sqlContext.sql("SELECT MACH_SER_NO, StartDate, EndDate, DateValid, Date, Max_SMU, endOvrSpd1, engPreLubeOvr1, grdLvlShut1, brkRealted1, hiOilTemp1, hiFuelWtr1, idlStndRely1, payloadOvr1, ( endOvrSpd1 + engPreLubeOvr1 + grdLvlShut1 + brkRealted1 + hiOilTemp1 + hiFuelWtr1 + idlStndRely1 + payloadOvr1 ) as TotalAbuseEvents FROM eventsCalTable ORDER BY MACH_SER_NO, StartDate, EndDate, DateValid ") 
 
	finalEventsOutput.registerTempTable("finalEventsOutput")

//val eventsReult = finalEventsOutput.map(x => x.mkString(","))

finalEventsOutput

}

}

/************************************************ Post Event ERC **************************************************************/


def getSMU_Event( x : Any ) : Option[Double] = {

	try{  Some( (( Math.floor( x.toString.toDouble /100 ) * 100) + 100 ).toDouble  )}
	catch{
		case p: java.lang.NullPointerException => Some( 100.0 )
	        case e : java.lang.Exception => Some( 100.0 ) 		
	 }
}

sqlContext.registerFunction("getSMU_Event" , getSMU_Event( _ : Any ))
finalEventsOutput.registerTempTable("finalEventsOutput")
 
val postEventDataResults = sqlContext.sql("SELECT MACH_SER_NO, StartDate, EndDate, DateValid, Date, Max_SMU, endOvrSpd1, engPreLubeOvr1, grdLvlShut1, brkRealted1, hiOilTemp1, hiFuelWtr1, idlStndRely1, payloadOvr1, TotalAbuseEvents , getSMU_Event(Max_SMU) AS SMU_Event  FROM finalEventsOutput ")

postEventDataResults.registerTempTable("postEventDataResults")


val EventsDataSummary = sqlContext.sql( "SELECT  MACH_SER_NO, StartDate, EndDate, SMU_Event , sum(endOvrSpd1) AS Sum_endOvrSpd1 , sum(engPreLubeOvr1) AS Sum_engPreLubeOvr1 , sum(grdLvlShut1 ) AS Sum_grdLvlShut1 , sum(brkRealted1) AS Sum_brkRealted1 , sum(hiOilTemp1) AS Sum_hiOilTemp1 , sum(hiFuelWtr1) AS Sum_hiFuelWtr1 ,  sum(idlStndRely1) AS Sum_idlStndRely1 , sum(payloadOvr1) AS Sum_payloadOvr1 , sum(TotalAbuseEvents) AS Sum_TotalAbuseEvents  FROM  postEventDataResults GROUP BY  MACH_SER_NO, StartDate, EndDate, SMU_Event  " )


EventsDataSummary.registerTempTable("EventsERCResultVariables")

val payload_Exception_Events_variable = sqlContext.sql("SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload ,  Avg_Sum_PAYLOADWEIGHT ,  Avg_Sum_SHIFTCOUNT ,  Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_sum_Overload_1_1 , Avg_Sum_TripTime ,  Avg_Sum_Pyld_LdTrvSpd ,  Avg_Sum_FCR_LTRS_MINS ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalPercent_Datalogger ,CriticalHours , CriticalDays , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents FROM Payload_Exception_Variables LEFT OUTER JOIN EventsERCResultVariables ON MachSer = MACH_SER_NO AND Bin = SMU_Event ")

payload_Exception_Events_variable.registerTempTable("payload_Exception_Events_variable")




/********************************************************   Fluid ERC   ***************************************************************



****************************************************************************************/

case class Machine_Fluid (Machine_No : String , Start_Date : java.sql.Date,  End_Date : java.sql.Date, Date_Valid : java.sql.Date)

case class Fluid_Fluid (Serial_No : String , Date : java.sql.Date,  SMU : Double, analysisResult: String, fluidAddedQnty : String, filterChanged: String, fluidChanged: String, primeCmpnt: String )

case class MyTemp_Fluid( Machine_No : Option[String] ,   Start_Date : Option[java.sql.Date], End_Date : Option[java.sql.Date] , Bin : Option[Double] ,  fluidBadSampleCount : Option[Double] ,fluidSampleCount : Option[Double] , fluidSampleSeverity : Option[Double] , sum_fluidAddedQnty : Option[Double] , fluidChangedCount : Option[Double] , filterChangedCount : Option[Double]  , flilterChangeRunningCount : Option[Double] )




/***********************************************************    Fluid ERC code  ******************************************************/



object FluidERC {


def repairDate( str : String ) : java.sql.Date = { 
 	val dateString = str
	val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
	new java.sql.Date (format.parse( dateString ).getTime() )
}


	def getUtilDate( str : String ) : java.util.Date = { 
		val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
		format.parse( str )
	}

def getBadSampleScore ( str : String ) : Double = {
    if( str.equals("NAR") ) 
        0
    else 
        1
}

def getSeverityScore ( str : String ) : Double = {
        if(str.equals("MC")) 
           1
        else if( str.equals("NAR") )
           0
        else 
          2
}


def getBin ( str : Any ) : Option[Double] = {
	try{	
	val x = (Math.floor(str.toString.toDouble/100)*100)+100
	  Some(x)
	 }
	catch { case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None }
}


	
def myDouble( x : Any ) : Double = {

	try{ x.toString.toDouble }
	catch {
		case p : java.lang.NullPointerException => 0.0
		case e : java.lang.Exception  => 0.0 }
}


	
def getDate ( str : Any ) : Option[java.sql.Date] = {
	
	try{
	 	Some(java.sql.Date.valueOf( str.toString ))
	}
	catch { 
		case e: java.lang.Exception  => None }
}

def getDouble ( str : Any ) : Option[Double ]= {
	try {
		Some(str.toString.toDouble)
	}
	catch{
		case e : java.lang.NullPointerException => None
		case e : java.lang.Exception  => None
		
	}
}

def getString (str : Any ) : Option[String] = {
	try {Some(str.toString)}
	catch { 
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception  => None }
}

/******************************************************************************************************************************


**************************************************************************************************************************/
def evaluteFluidERC ( sc : SparkContext , args : Array[String] ) :  org.apache.spark.rdd.RDD[String] = {
	


val fluidDataRaw = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/fluid/fluid_data.csv")
val rawMachineList = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/fluid/machine.csv").filter(!_.contains("StartDate"))

val rawFluidData = fluidDataRaw.filter(!_.contains("Date"))

val fluidHeader = fluidDataRaw.take(1)(0).split(",")

val indexOfSerial = fluidHeader.indexOf( "/sample/mfrSerialNumber" )
val indexOfDate = fluidHeader.indexOf("Date")
val indexOfSMU = fluidHeader.indexOf("/sample/serviceMeter")
val indexOfAnalysisResult = fluidHeader.indexOf("/sample/analysisResult")
val indexOffluidAddedQnty = fluidHeader.indexOf("/sample/fluidAddedQuantity")
val indexOffilterChanged = fluidHeader.indexOf("/sample/filterChanged")
val indexOffluidChanged = fluidHeader.indexOf("/sample/fluidChanged")
val indexOfprimeCmpnt = fluidHeader.indexOf("PRIM_CMPNT_DESC")


val fluidData = rawFluidData.map( line => { val words = line.split(",");  Fluid_Fluid( words(indexOfSerial), repairDate(words(indexOfDate)), words(indexOfSMU).toDouble, words(indexOfAnalysisResult),  words(indexOffluidAddedQnty) ,  words(indexOffilterChanged),  words(indexOffluidChanged),  words(indexOfprimeCmpnt) ) } )





val  machineRDD = rawMachineList.map( x => { val w = x.split(",")
			  
			   /* println( w(1) + "    " + w(2) ) */
			  val sqlStartDate = java.sql.Date.valueOf( w(1).toString )
			  val sqlEndDate = java.sql.Date.valueOf( w(2).toString )
			  var dateList = List[(java.sql.Date , java.sql.Date , java.sql.Date)]( (sqlStartDate, sqlStartDate , sqlEndDate ) )
			  			   
			  val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
			  val c = java.util.Calendar.getInstance()
			  c.setTime( sdf.parse(w(1).toString) )
			  c.add( java.util.Calendar.DATE , 1 )
			  var dt = sdf.format(c.getTime())
			  while( java.sql.Date.valueOf(dt)  before sqlEndDate ) {
				
			dateList = dateList ::: List[( java.sql.Date , java.sql.Date , java.sql.Date )]( (java.sql.Date.valueOf(dt) , sqlStartDate , sqlEndDate ))
			    c.add( java.util.Calendar.DATE , 1 )
			    dt = sdf.format(c.getTime())
			  } 

		 (  w(0) ,  dateList ::: List[(java.sql.Date , java.sql.Date , java.sql.Date )] (  ( java.sql.Date.valueOf(dt) , sqlStartDate , sqlEndDate ) ) )

		        } ) 

val masterFlatRDD = machineRDD.flatMapValues( x => x ) 


val machineData = masterFlatRDD.map( x =>  Machine_Fluid ( x._1 , x._2._2 , x._2._3  , x._2._1) ) 

fluidData.registerTempTable("FluidTable")

machineData.registerTempTable("MachineTable")




sqlContext.registerFunction("getBin" , getBin( _ : Any ) )

val machineJoinedFluid = sqlContext.sql("SELECT Machine_No ,Start_Date , End_Date , Date_Valid , Date , SMU , analysisResult ,fluidAddedQnty , filterChanged , fluidChanged , primeCmpnt   FROM MachineTable LEFT JOIN FluidTable ON  Machine_No = Serial_No AND Date_Valid = Date ").distinct



machineJoinedFluid.registerTempTable("machineOrdered")
 


val machineOrderedRDD = sqlContext.sql("SELECT Machine_No ,Start_Date , End_Date , Date_Valid , Date , SMU , analysisResult ,fluidAddedQnty , filterChanged , fluidChanged , primeCmpnt , IF( analysisResult ='NAR' , 0 , 1 ) AS badSampleFlag , IF(analysisResult = 'MC' , 1 , IF( analysisResult = 'NAR' , 0 , 2  )) AS SeverityScore , getBin(SMU) AS Bin FROM machineOrdered ORDER BY  Machine_No ,Start_Date , End_Date , Date_Valid ") 
 
machineOrderedRDD.registerTempTable("machineOrderedTable")


val joinedRDDAggregate = sqlContext.sql("SELECT Machine_No, Start_Date, End_Date, Bin, sum(badSampleFlag) as fluidBadSampleCount, count(badSampleFlag) as fluidSampleCount, sum(SeverityScore) as fluidSampleSeverity, sum(fluidAddedQnty) as sum_fluidAddedQnty, sum(fluidChanged) as fluidChangedCount, sum(filterChanged) as filterChangedCount FROM machineOrderedTable	GROUP BY Machine_No, Start_Date,End_Date, Bin ORDER BY Machine_No, Start_Date, End_Date, Bin")


val zippedAggregate = joinedRDDAggregate.zipWithIndex.map( x => ( x._1(0).toString , (x._1 , myDouble( x._1(9) ), x._2 )) ) 


val zippedAggregateValues = zippedAggregate.combineByKey( 
				value => List( value ) , 
	 ( aggr : List[(org.apache.spark.sql.Row, Double , Long ) ]  , value ) =>  ( aggr ::: List( value ) ) ,

( aggr1 : List[(org.apache.spark.sql.Row, Double , Long ) ] , aggr2  : List[(org.apache.spark.sql.Row, Double , Long ) ]  )   => aggr1 ::: aggr2  ).mapValues( x => x.sortWith( (p,q ) => p._3 < q._3 ) ).mapValues( x => {

			var list = List[Double]()
			for( a <- x ){
			   list = list ::: List[Double]( a._2)
			}
			list = list.scanLeft( 0.0)( _ + _ ).tail
			//var l = List[(org.apache.spark.sql.Row, Double , Long ,Double)]()
			
  val myList : List[( (org.apache.spark.sql.Row, Double , Long ) , Double )] = x.zip( list).map( p => (p._1 , p._2))			

	myList	
	}
)	





val zippedValues =  zippedAggregateValues.flatMapValues( x => x ).map( x => ( x._2._1._1 , x._2._2 ) ).map( x => ( getString(x._1(0) ) , getDate( x._1(1) ) , getDate( x._1(2) ) , getDouble( x._1(3) ) , getDouble( x._1(4) ) , getDouble( x._1(5) ), getDouble( x._1(6) ) , getDouble( x._1(7) ),getDouble( x._1(8) ), getDouble( x._1(9) ) , getDouble( x._2)     ) )                                                                                           



val aggregateMapped = zippedValues.map( x => MyTemp_Fluid( x._1 , x._2 , x._3 , x._4 , x._5 , x._6 , x._7 ,x._8 , x._9 , x._10 , x._11 ) )

aggregateMapped.registerTempTable( "aggregateMapped" )

val aggregateformula = sqlContext.sql( "SELECT  Machine_No, Start_Date, End_Date, Bin AS Bin_Fluid ,fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount , (fluidBadSampleCount /fluidSampleCount ) * 100.0  AS BadSamplePercentage , 100 AS ActualEngineHours,fluidBadSampleCount / 100.0 AS FluidBadSamplePerSMu  ,  fluidSampleSeverity / fluidSampleCount  AS FluidSampleSeverityScore , filterChangedCount / 100.0 AS FilterChangePerHours , flilterChangeRunningCount / Bin AS CumulativeFilterChangePerHours    FROM aggregateMapped WHERE Bin IS NOT NULL ORDER BY Machine_No, Start_Date, End_Date, Bin ")



/* outputFeilds 

Machine_No, Start_Date, End_Date, Bin,fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,BadSamplePercentage, ActualEngineHours ,FluidBadSamplePerSMu  , FluidSampleSeverityScore ,FilterChangePerHours , CumulativeFilterChangePerHours

                                         Part - II 


*/


case class FluidSummary_final( Machine_Fluid : String ,  Bin_Fluid : Option[Double] , TBFC : Int ) 

val machine_Fluid_Bin_date = sqlContext.sql("SELECT  Machine_No ,Date ,Bin FROM machineOrderedTable where  filterChanged = 1 ORDER BY Machine_No ,Date ")

def dayDiff_Int( day1 : Any , day2 : Any ) : Int = {

	
			
	((java.sql.Date.valueOf(day2.toString).getTime - java.sql.Date.valueOf( day1.toString ).getTime ) / (24 * 60 * 60 *1000 )).toString.toInt

}


val MFBlagged = machine_Fluid_Bin_date.zipWithIndex.
			flatMap( x => 
			    {
			      ( 0 to 1 ).map( i => ( x._2 - i , ( x._2 , x._1 )  ) ) 
			    }  
			 ).
		  combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ]( value ) , 
				( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value ) => aggr ::: List( value ) , 
	( aggr1 :  List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 
                  ).
		  mapValues( x => x.sortWith( ( p , q ) => p._1 < q._1 ) ) .
		  mapValues( x => {
					if(x.length == 1){
						( x(0)._1,  x(0)._2 ,  0.toString.toInt  ) 
					}				
					else if( x(0)._2(0).toString == x(1)._2(0).toString ){
						
						( x(1)._1 , x(1)._2 , dayDiff_Int( x(0)._2(1) , x(1)._2(1) ) ) 
					}
					else {
						( x(1)._1 , x(1)._2 , 0.toString.toInt )
					}

		}).filter( x => x._1 != x._2._1 ). 
		map( x =>  ( x._2._2(0).toString , x._2 ) ).
		combineByKey( value => List( value ) , 
				( aggr : List[(Long , org.apache.spark.sql.Row, Int ) ] , value ) => aggr ::: List( value ) , 
	( aggr1 :  List[(Long , org.apache.spark.sql.Row , Int ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row, Int ) ] ) => aggr1 ::: aggr2 
                  ).
		mapValues( x => x.toList.sortWith (( p, q ) => p._1 < q._1 ) ).
		mapValues ( x => {
			var list : List[Int] = List[Int]()
			var binList : List[Option[Double]] = List[ Option[Double]]()
			for ( p <- x ) {  list = list ::: List[Int](p._3.toString.toInt)
					  binList = binList ::: List[ Option[Double] ] ( getDouble( p._2(2) ) )  
					}
			
			list.scanLeft(0)( _ + _ ).tail.zip( binList )
		
						
		}). flatMapValues ( x => x ) .map( x => FluidSummary_final( x._1 , x._2._2 , x._2._1 ) )

MFBlagged.registerTempTable( "MFBlagged" )

val TBFCResult = sqlContext.sql("SELECT Machine_Fluid , Bin_Fluid , avg(TBFC) AS TBFC FROM MFBlagged GROUP BY Machine_Fluid , Bin_Fluid ")




/**********************************************************  POst FLuid INtegration Joins *****************************/


/***************************************************************************************************************************
	fluid part - 1 

val aggregateformula = 

  Machine_No, Start_Date, End_Date, Bin,fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,  BadSamplePercentage ,  ActualEngineHours, FluidBadSamplePerSMu  ,  FluidSampleSeverityScore ,  FilterChangePerHours ,  CumulativeFilterChangePerHours 

*************************************************************************************************************/

aggregateformula.registerTempTable("aggregateformula")

val payload_Exception_Events_Fluid_variables = sqlContext.sql(" SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload  , Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalHours , CriticalDays  , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents , fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,  BadSamplePercentage ,  ActualEngineHours, FluidBadSamplePerSMu  ,  FluidSampleSeverityScore ,  FilterChangePerHours ,  CumulativeFilterChangePerHours   FROM  payload_Exception_Events_variable LEFT OUTER JOIN aggregateformula  ON  MachSer = Machine_No AND Bin = Bin_Fluid ")



payload_Exception_Events_Fluid_variables.registerTempTable("payload_Exception_Events_Fluid_variables")

/*************************************************************** Fluid Part -2 ********************************************************/

 TBFCResult.registerTempTable("TBFCResult") 




val payload_Exception_Events_Fluid_variables_TBFC = sqlContext.sql("SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload  , Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalHours , CriticalDays  , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents , fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,  BadSamplePercentage ,  ActualEngineHours, FluidBadSamplePerSMu  ,  FluidSampleSeverityScore ,  FilterChangePerHours ,  CumulativeFilterChangePerHours  FROM payload_Exception_Events_Fluid_variables LEFT OUTER JOIN  TBFCResult ON MachSer = Machine_Fluid AND Bin = Bin_Fluid ")

payload_Exception_Events_Fluid_variables_TBFC.registerTempTable("payload_Exception_Events_Fluid_variables_TBFC")


/*******************************************************************************************************************************
Cumulative ERC  Working 

******************************************************************************************************************************/
//Cumulative Records 

case class Cumulative_class ( Serial : Option[ String] , VIMS : Option[java.sql.Timestamp ] , SMU : Option[Double] , CUMID : Option[Double]  , Description : Option[String] , DataValue : Option[Double] , ErrorCount : Option[Double] , UnitId : Option[Double] , scaleFactor : Option[Double] , date : Option[java.sql.Date] ) 


case class CumulativeIdle ( SerialIdle : Option[String] , VIMS_Idle : Option[java.sql.Date] , SMU_Idle : Option[Double] , deltaSMUIdle : Option[Double] , DeltaIdleTime : Option[Double] )
 
case class CumulativeEST ( SerialEST : Option[String] , VIMS_EST : Option[java.sql.Date] , SMU_EST : Option[Double] , deltaSMUEST : Option[Double] , DeltaESTTime : Option[Double] )

case class CumulativeStarts( SerialStarts : Option[String] , VIMS_Starts : Option[java.sql.Date] , SMU_Starts : Option[Double] , deltaSMUStarts : Option[Double] , DeltaStartsTime : Option[Double] )


case class IdleSummaryTemp ( SerialIdle : Option[String] , dateIdle : Option[java.sql.Date] , SMUIdle : Option[Double] , sum_deltaSMUIdle : Option[Double] , sum_SMUIdleTime : Option[Double] , SMUEngineIdleRatio : Option[Double] , dayDiffIdle : Option[Long] , Avg_SMU : Option[Double] , Avg_Value : Option [Double] , avg_value_ : Option[Double] , bin1 : Option[Double]  )

case class ESTSummaryTemp( SerialEST : Option[String] , dateEST : Option[java.sql.Date] , SMUEST : Option[Double] , sum_deltaSMUEst : Option[Double] , sum_SMUEstTime : Option[Double] , LitrePerHour : Option[Double] , dayDiffEST : Option[Long] , Avg_SMU : Option[Double] , Avg_Value : Option[Double] , bin2 : Option[Double]  )


case class StartsSummaryTemp( SerialStarts : Option[String] , dateStarts : Option[java.sql.Date] , SMUStarts : Option[Double] , sum_deltaSMUStarts : Option[Double] ,  sum_SMUStartsTime : Option[Double] , dayDiff : Option[Long] , Avg_SMU : Option[Double] , Avg_Value : Option[Double] , avg_value_ : Option[Double] , bin3 : Option[Double]   )

case class masterTableTemp( Serial : String , startDate : java.sql.Date , EndDate : java.sql.Date , dateValid : java.sql.Date )


/********************************************   cumulative Codes  ************************************/



def dropHeader(data: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
         	data.mapPartitionsWithIndex((idx, lines) => {
           		if (idx == 0) {
             			lines.drop(1)
           		}
           		lines
         	}	)
  	}		
		
	
	
 	def getString ( x  : Any ) : Option[String] = {
           try{ Some(x.toString) }
	   catch{ case p : java.lang.NullPointerException => None 
		case e : java.lang.Exception => None }

  	}

 	def getDouble( str : Any ) : Option[Double] = {
		
		try  { Some( str.toString.toDouble ) }
		catch { case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception => None  }
    	}


	def sqlDate( d : Any )  : Option[ java.sql.Date] = {

        	try{  Some(java.sql.Date.valueOf( d.toString )) }
        	catch{  case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception => None}
    
    	}


	def sqlTimestamp( d : Any ) : Option[java.sql.Timestamp] = {
		
		try {
			Some( java.sql.Timestamp.valueOf( d.toString.trim ) ) 
		}
		catch { case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception => None
		}
	}

	def mySqlDate( str : Any ) : Option[java.sql.Date] = {
		try{   Some( java.sql.Date.valueOf(str.toString.split(" ")(0) ) )  }
		catch{  case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception =>  None }

	}

	
	def dayDiff( d1 : Any , d2 : Any ) : Option[Long] = {
		try{
			Some( (java.sql.Date.valueOf( d1.toString ) .getTime - java.sql.Date.valueOf( d2.toString ) .getTime ) / (1000 * 24 * 60 * 60))
		
		}
		catch{  case p : java.lang.NullPointerException => None 
			case e : java.lang.Exception => None }
	}

	def max_SMU( a : Double , b : Double , c : Double  ) : Option[Double] = { 
		if(List( a , b , c  ).max == 0 )
			None
		else 
			Some(List( a , b , c  ).max)
   	}
	

       val cumulativeFile = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/Cumulative/cumulative.csv")
		val cumulativeWithOutHeader = dropHeader( cumulativeFile )

		val machineStartDates = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/Cumulative/machine.csv")
		
		val headerList = cumulativeFile.take(1)(0).split(",").toList
		val indexOfSerial = headerList.indexOf("SERIAL_NUMBER")
		val indexOfVIMS = headerList.indexOf("VIMSTIME")
		val indexOfSMU = headerList.indexOf("SMU")
		val indexOfCUMID = headerList.indexOf("CUMID")
		val indexOfDescription = headerList.indexOf("DESCRIPTION")
		val indexOfDataValue = headerList.indexOf("DATAVALUE")
		val indexOfErrorCount = headerList.indexOf("ERRORCOUNT")
		val indexOfUnitId = headerList.indexOf("UNIT_ID")
		val indexOfScaleFactor = headerList.indexOf("SCALEFACTOR")
		val indexOfDate = headerList.indexOf("date") 			
		
		
		
		val idleCumulative = cumulativeWithOutHeader.filter( x => x.split(",")(indexOfDescription).contains("Idle") &&  ! x.split(",")(indexOfDescription).contains("Fuel")  )

		val estTotalFuelCumulative = cumulativeWithOutHeader.filter( x => x.split(",")(indexOfDescription).contains("Est Total Fuel Used") )

		val startsCumulative = cumulativeWithOutHeader.filter( x => x.split(",")(indexOfDescription).contains("Starts") )


		val mappedIdleCumulative = idleCumulative.map( line =>{ val word = line.split(",") 
								Cumulative_class( getString(word(indexOfSerial)) ,  sqlTimestamp( word(indexOfVIMS) ) , getDouble( word(indexOfSMU)) ,getDouble( word(indexOfCUMID)) , getString(word(indexOfDescription))  ,getDouble( word(indexOfDataValue)) ,getDouble( word(indexOfErrorCount)) ,getDouble( word(indexOfUnitId)) , getDouble( word(indexOfScaleFactor)) , sqlDate(word(indexOfDate))   )
							    }
						  )


		val mappedEstTotalCumulative = estTotalFuelCumulative.map( line =>{ val word = line.split(",") 
								Cumulative_class( getString(word(indexOfSerial)) ,  sqlTimestamp( word(indexOfVIMS) ) , getDouble( word(indexOfSMU)) ,getDouble( word(indexOfCUMID)) , getString(word(indexOfDescription))  ,getDouble( word(indexOfDataValue)) ,getDouble( word(indexOfErrorCount)) ,getDouble( word(indexOfUnitId)) , getDouble( word(indexOfScaleFactor)) , sqlDate(word(indexOfDate))   )
							    }
						  )

		val mappedStartsCumulative = startsCumulative.map( line =>{ val word = line.split(",") 
								Cumulative_class( getString(word(indexOfSerial)) ,  sqlTimestamp( word(indexOfVIMS) ) , getDouble( word(indexOfSMU)) ,getDouble( word(indexOfCUMID)) , getString(word(indexOfDescription))  ,getDouble( word(indexOfDataValue)) ,getDouble( word(indexOfErrorCount)) ,getDouble( word(indexOfUnitId)) , getDouble( word(indexOfScaleFactor)) , sqlDate(word(indexOfDate))   )
							    }
						  )	


		


		mappedIdleCumulative.registerTempTable("mappedIdleCumulative")

		val orderedIdleCumulative = sqlContext.sql( "SELECT * FROM mappedIdleCumulative ORDER BY Serial , VIMS " ).zipWithIndex.
			    flatMap( x => { (0 to 1 ).map( i =>  (x._2 - i , ( x._2 , x._1)  ) )  } ).
			    combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith( ( p, q ) => p._1 < q._1 ) ).
mapValues(  x => { 
		if( x.length == 1 )
			( x(0)._1 , x(0)._2 , None , None )
		else if( x(0)._2(0) == x(1)._2(0)  )
			( x(1)._1 , x(1)._2 , Some(x(1)._2(2).toString.toDouble - x(0)._2(2).toString.toDouble) , Some(x(1)._2(5).toString.toDouble - x(0)._2(5).toString.toDouble  ) )
		else
		       (  x(1)._1 , x(1)._2 , None , None  ) 


  }  ).filter( x => x._1 != x._2._1 ).sortBy( _._1 ).
   map( x => ( x._2._2 , x._2._3.getOrElse("None") , x._2._4.getOrElse("None") ) ).
  map( x => (  getString( x._1(0) ) , mySqlDate( x._1(1) ) , getDouble( x._1(2) ) , getDouble( x._2 ) , getDouble( x._3)  )    )




	mappedEstTotalCumulative.registerTempTable("mappedEstTotalCumulative")

	val orderedEstTotalCumulative = sqlContext.sql( "SELECT * FROM mappedEstTotalCumulative ORDER BY Serial , VIMS " ).zipWithIndex.
			    flatMap( x => { (0 to 1 ).map( i =>  (x._2 - i , ( x._2 , x._1)  ) )  } ).
			    combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith( ( p, q ) => p._1 < q._1 ) ).
mapValues(  x => { 
		if( x.length == 1 )
			( x(0)._1 , x(0)._2 , None , None )
		else if( x(0)._2(0) == x(1)._2(0)  )
			( x(1)._1 , x(1)._2 , Some(x(1)._2(2).toString.toDouble - x(0)._2(2).toString.toDouble) , Some(x(1)._2(5).toString.toDouble - x(0)._2(5).toString.toDouble  ) )
		else
		       (  x(1)._1 , x(1)._2 , None , None  ) 
  }  ).filter( x => x._1 != x._2._1 ).sortBy( _._1 ).
  map( x => ( x._2._2 , x._2._3.getOrElse("None") , x._2._4.getOrElse("None") ) ).
  map( x => (  getString( x._1(0) ) , mySqlDate( x._1(1) ) , getDouble( x._1(2) ) , getDouble( x._2 ) , getDouble( x._3)  )    )




	mappedStartsCumulative.registerTempTable("mappedStartsCumulative")

	val orderedStartsCumulative = sqlContext.sql( "SELECT * FROM mappedStartsCumulative ORDER BY Serial , VIMS " ).zipWithIndex.
			    flatMap( x => { (0 to 1 ).map( i =>  (x._2 - i , ( x._2 , x._1)  ) )  } ).
			    combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith( ( p, q ) => p._1 < q._1 ) ).
mapValues(  x => { 
		if( x.length == 1 )
			( x(0)._1 , x(0)._2 , None , None )
		else if( x(0)._2(0) == x(1)._2(0)  )
			( x(1)._1 , x(1)._2 , Some(x(1)._2(2).toString.toDouble - x(0)._2(2).toString.toDouble) , Some(x(1)._2(5).toString.toDouble - x(0)._2(5).toString.toDouble  ) )
		else
		       (  x(1)._1 , x(1)._2 , None , None  ) 
}  ).filter( x => x._1 != x._2._1 ).sortBy( _._1 ).
	map( x => ( x._2._2 , x._2._3.getOrElse("None") , x._2._4.getOrElse("None") ) ).
        map( x => (  getString( x._1(0) ) , mySqlDate( x._1(1) ) , getDouble( x._1(2) ) , getDouble( x._2 ) , getDouble( x._3)  )    )




	
	val IdleSchema = orderedIdleCumulative.map( x => CumulativeIdle( x._1 , x._2 , x._3 , x._4 , x._5 ) )

	val ESTSchema = orderedEstTotalCumulative.map( x => CumulativeEST( x._1 , x._2 , x._3 , x._4 , x._5 ) )

	val StartsSchema = orderedStartsCumulative.map( x => CumulativeStarts( x._1 , x._2 , x._3 , x._4 , x._5 ) )

	IdleSchema.registerTempTable("IdleSchema")

	ESTSchema.registerTempTable("ESTSchema")

	StartsSchema.registerTempTable("StartsSchema")


	val IdleSummary = sqlContext.sql("SELECT SerialIdle , VIMS_Idle , max(SMU_Idle) , sum(deltaSMUIdle) , sum(DeltaIdleTime ) , sum(deltaSMUIdle) / sum(DeltaIdleTime )  As SMUEngineIdleRatio  FROM IdleSchema GROUP BY SerialIdle , VIMS_Idle ORDER BY SerialIdle , VIMS_Idle ")


	val ESTSummary = sqlContext.sql("SELECT SerialEST , VIMS_EST , max(SMU_EST) , sum(deltaSMUEST) , sum(DeltaESTTime) , sum(DeltaESTTime) / sum(deltaSMUEST)  As LitrePerHour FROM ESTSchema GROUP BY SerialEST , VIMS_EST ORDER BY SerialEST , VIMS_EST ")

	val StartsSummary = sqlContext.sql("SELECT SerialStarts , VIMS_Starts , max(SMU_Starts) , sum(deltaSMUStarts) , sum(DeltaStartsTime) FROM StartsSchema GROUP BY SerialStarts , VIMS_Starts ORDER BY SerialStarts , VIMS_Starts ")	


	
	val lagIdleSummary = IdleSummary.zipWithIndex.flatMap{ x => (0 to 1 ).map(i => ( x._2 - i , (x._2 , x._1 )  ) ) }.
				combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith((p,q) => p._1 < q._1 ) ).
mapValues( x => { 
		if( x.length == 1 ) 
			( x(0)._1 ,x(0)._2 , None )
		else if( x(0)._2(0) == x(1)._2(0) )
			(  x(1)._1,x(1)._2 , dayDiff( x(1)._2(1) , x(0)._2(1) ) )
		else
			( x(1)._1 ,x(1)._2 , None )	

  }).filter( x => x._1 != x._2._1 ).map( x => ( x._2._2 , x._2._3 )).map( x => {

	val Avg_SMU = try {   Some( x._1(3).toString.toDouble / x._2.get )  } 
			catch { case e : java.util.NoSuchElementException => None 
				case e : java.lang.Exception => None }
	val Avg_Value = try { Some( x._1(4).toString.toDouble / x._2.get )  } catch {
						case e : java.util.NoSuchElementException => None
						case e : java.lang.Exception => None }
	val avg_value_ = try {  if( Avg_Value.get < 0  || Avg_Value.get > Avg_SMU.get  ) { Some(0.0)  } else { Avg_Value }   } catch { 
		case e : java.lang.Exception => None 
		case e : java.util.NoSuchElementException => None }
	val bin1 =   try{   Some (( Math.floor(x._1(2).toString.toDouble / 100 ) * 100 ) + 100)    }catch{ 
			case e : java.util.NoSuchElementException => None				
			case e : java.lang.Exception => None }
		
		( x._1 , x._2 , Avg_SMU , Avg_Value , avg_value_  , bin1 )

        } )


	
val lagESTSummary = ESTSummary . zipWithIndex.flatMap{ x => (0 to 1 ).map(i => ( x._2 - i , (x._2 , x._1 )  ) ) }.
				combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith((p,q) => p._1 < q._1 ) ).
mapValues( x => { 
		if( x.length == 1 ) 
			( x(0)._1 ,x(0)._2 , None )
		else if( x(0)._2(0) == x(1)._2(0) )
			(  x(1)._1,x(1)._2 , dayDiff( x(1)._2(1) , x(0)._2(1) ) )
		else
			( x(1)._1 ,x(1)._2 , None )	

  }).filter( x => x._1 != x._2._1 ).
    map( x => ( x._2._2 , x._2._3 ) ).
    map( x => { 			   
		val Avg_SMU = try { Some(x._1(3).toString.toDouble / x._2.get) } catch { case e : java.lang.Exception => None  }
		val Avg_Value = try { 
					if( (x._1(5).toString.toDouble / x._2.get) < 0 ||  ( x._1(5).toString.toDouble / x._2.get) > 100  )
						Some(0.0)
					else 
						 Some(x._1(5).toString.toDouble / x._2.get)
				    } catch { case e : java.lang.Exception => None }
		val bin2 = try{  Some ((Math.floor( x._1(2).toString.toDouble /100 )* 100 ) + 100 )   }
			   catch { case e : java.lang.Exception => None }
		( x._1 , x._2 , Avg_SMU , Avg_Value , bin2 )
	} )



	val lagStartsSummary = StartsSummary.zipWithIndex.flatMap{ x => (0 to 1 ).map(i => ( x._2 - i , (x._2 , x._1 )  ) ) }.
				combineByKey( value => List[(Long , org.apache.spark.sql.Row ) ](value ) , ( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value : (Long , org.apache.spark.sql.Row )  ) => aggr ::: List[(Long , org.apache.spark.sql.Row ) ]( value ) , ( aggr1 : List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 ).
mapValues( x => x.sortWith((p,q) => p._1 < q._1 ) ).
mapValues( x => { 
		if( x.length == 1 ) 
			( x(0)._1 ,x(0)._2 , None )
		else if( x(0)._2(0) == x(1)._2(0) )
			(  x(1)._1,x(1)._2 , dayDiff( x(1)._2(1) , x(0)._2(1) ) )
		else
			( x(1)._1 ,x(1)._2 , None )	

  }).filter( x => x._1 != x._2._1 ).sortBy(_._1).map( x => ( x._2._2 , x._2._3 )).
 map( x => {
		val Avg_SMU = try{ Some(x._1(3).toString.toDouble / x._2.get) }catch{ 
		case p : java.util.NoSuchElementException =>  None
		case e : java.lang.Exception => None 
		case p : java.util.NoSuchElementException =>  None
		 }
		
		val Avg_Value = try { Some( (x._1(4).toString.toDouble / x._2.get) ) } catch { 
			
			case e : java.lang.Exception => None }
		val avg_value_ = try { if( Avg_Value == None  )
						None
					else if( Avg_Value.get < 0 ) 
						Some(0.0) 
					else 
						Some( ( Avg_Value.get ) ) 
				     }
				 catch {        case p : java.util.NoSuchElementException =>  None
						case p : java.lang.NullPointerException => None 
						case e : java.lang.Exception => None }
		
		val bin3 = try{  Some ((Math.floor( x._1(2).toString.toDouble /100 )* 100 ) + 100 )   }
			   catch { case e : java.lang.Exception => None
					case p : java.util.NoSuchElementException =>  None				
				 }
		( x._1 , x._2 , Avg_SMU , Avg_Value , avg_value_ , bin3 )
		 
    	  })

	val lagIdleSummarySchema = lagIdleSummary.map( x => IdleSummaryTemp( getString(x._1(0)) , sqlDate(x._1(1)) , getDouble( x._1(2) ) , getDouble( x._1(3) ) , getDouble( x._1(4) ) , getDouble( x._1(5) ) ,  x._2 , x._3 , x._4 , x._5 , x._6  )  ) 	

	
	val lagESTSummarySchema = lagESTSummary.map( x => ESTSummaryTemp( getString(x._1(0)) , sqlDate(x._1(1)) , getDouble( x._1(2) ) , getDouble( x._1(3) ) , getDouble( x._1(4) ) , getDouble( x._1(5) ) ,  x._2 , x._3 , x._4 , x._5 )  )

	
	val lagStartsSummarySchema  = lagStartsSummary.map( x => StartsSummaryTemp( getString(x._1(0)) , sqlDate(x._1(1)) , getDouble( x._1(2) ) , getDouble( x._1(3) ) , getDouble( x._1(4) ) , x._2  , x._3 , x._4 , x._5 , x._6) ) 


	
	val machineHead = machineStartDates.take(1)(0).split(",")

	val indexOfMachine = machineHead.indexOf("MACH_SER_NO")
	val indexOfStartDate = machineHead.indexOf("StartDate")
	val indexOfEndDate = machineHead.indexOf("EndDate")
	
	val machineDatesWithOutHeader = dropHeader(machineStartDates )


	val  dateTable = machineDatesWithOutHeader.map( x => { val w = x.split(",")
			  
	 /* println( w(1) + "    " + w(2) ) */
	val sqlStartDate = java.sql.Date.valueOf( w(indexOfStartDate).toString )
        val sqlEndDate = java.sql.Date.valueOf( w(indexOfEndDate).toString )
	var dateList = List[(java.sql.Date,java.sql.Date,java.sql.Date)]( (sqlStartDate, sqlEndDate , sqlStartDate) )
			  			   
	val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
	val c = java.util.Calendar.getInstance()
	c.setTime( sdf.parse(w(indexOfStartDate).toString) )
	c.add( java.util.Calendar.DATE , 1 )
	var dt = sdf.format(c.getTime())
	while( java.sql.Date.valueOf(dt)  before sqlEndDate ) {
				
	dateList = dateList ::: List[(java.sql.Date,java.sql.Date,java.sql.Date)]( (sqlStartDate, sqlEndDate,java.sql.Date.valueOf(dt)) )
				 c.add( java.util.Calendar.DATE , 1 )
				 dt = sdf.format(c.getTime())
			  } 
			  ( w(indexOfMachine) , dateList ::: List[(java.sql.Date,java.sql.Date,java.sql.Date)]((sqlStartDate, sqlEndDate,java.sql.Date.valueOf(dt)) ) )

		        } ) 

	val masterDates = dateTable.flatMapValues( x => x ).map( x => ( x._1 , x._2._1 , x._2._2 , x._2._3 ))	

	println( masterDates.count )
	
	val masterTableSchema = masterDates.map( x => masterTableTemp( x._1 , x._2 , x._3 , x._4 ) )


	masterTableSchema.registerTempTable("masterTable")

	lagIdleSummarySchema.registerTempTable( "lagIdleSummaryTable")

	lagESTSummarySchema.registerTempTable("lagESTSummaryTable")

	lagStartsSummarySchema.registerTempTable("lagStartsSummaryTable")


	val resJoin1 = sqlContext.sql("SELECT Serial, startDate , EndDate , dateValid , dateIdle , SMUIdle As MaxSMU1 , bin1 , avg_value_ As EngIdleTime FROM masterTable LEFT OUTER JOIN lagIdleSummaryTable ON Serial = SerialIdle AND dateValid = dateIdle    ORDER BY Serial,dateValid ")

	resJoin1.registerTempTable("resJoin1")

	val resJoin2 = sqlContext.sql("SELECT Serial, startDate , EndDate , dateValid , dateIdle , MaxSMU1 , bin1 , EngIdleTime , SMUEST As MaxSMU2 , bin2 , Avg_Value As FuelConsumed FROM resJoin1 LEFT OUTER JOIN lagESTSummaryTable ON Serial = SerialEST AND dateIdle = dateEST  ORDER BY Serial, dateValid ")

	resJoin2.registerTempTable("resJoin2")


	val resJoin3 = sqlContext.sql("SELECT  Serial, startDate , EndDate , dateValid , dateIdle , MaxSMU1 , bin1 , EngIdleTime ,MaxSMU2 , bin2 , FuelConsumed , SMUStarts As MaxSMU3 , avg_value_ As StartCount , bin3   FROM resJoin2 LEFT OUTER JOIN lagStartsSummaryTable ON Serial = SerialStarts AND dateValid = dateStarts ORDER BY Serial , dateValid ")

	resJoin3.registerTempTable("resJoin3")


	
	sqlContext.registerFunction( "max_SMU" , max_SMU( _ : Double , _ : Double , _ : Double   ) )


	val res = sqlContext.sql("SELECT  Serial,startDate , EndDate , dateValid , dateIdle , MaxSMU1 , bin1 , EngIdleTime ,MaxSMU2 , bin2 , FuelConsumed ,MaxSMU3 ,StartCount As EngStartCount , bin3, max_SMU( bin1 , bin2 , bin3)  AS bin    FROM resJoin3 ORDER BY Serial , startDate , EndDate , bin ")


/*
val res = sqlContext.sql("SELECT  Serial, startDate , EndDate , dateValid , dateIdle , MaxSMU1 , bin1 , EngIdleTime ,MaxSMU2 , bin2 , FuelConsumed ,MaxSMU3 ,StartCount As EngStartCount , bin3 , IF( MaxSMU1 >= MaxSMU2 , IF( MaxSMU1 > MaxSMU3 , MaxSMU1 , MaxSMU3 ) , IF( MaxSMU2 >= MaxSMU3 , MaxSMU2 , MaxSMU3  )  ) AS SMU , IF(bin1 > bin2 , IF(bin1 > bin3 , bin1 , bin3) , IF( bin2 > bin3 , bin2 , bin3 )   ) AS bin    FROM resJoin3 ORDER BY Serial , startDate , EndDate , bin ")
*/



	res.registerTempTable("res")

	val cumulativeResult = sqlContext.sql("SELECT Serial, startDate , EndDate , bin , sum(EngIdleTime) AS Max_EngineIdleTime  , sum(FuelConsumed) AS Max_FuelConsumed , sum(EngStartCount) Max_EngineStartCount   FROM res GROUP BY Serial, startDate , EndDate , bin ORDER BY Serial , bin " )

	cumulativeResult.registerTempTable("cumulativeResult")
	

/***************************************************************************************************************************************

                                    After Cumulative ERC calculations 

*****************************************************************************************************************************************/

/*      val payload_Exception_Events_Fluid_variables_TBFC = sqlContext.sql("SELECT MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload  , Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalHours , CriticalDays  , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents , fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,  BadSamplePercentage ,  ActualEngineHours, FluidBadSamplePerSMu  ,  FluidSampleSeverityScore ,  FilterChangePerHours ,  CumulativeFilterChangePerHours  FROM payload_Exception_Events_Fluid_variables LEFT OUTER JOIN  TBFCResult ON MachSer = Machine_Fluid AND Bin = Bin_Fluid ")
                                                                                  */


	payload_Exception_Events_Fluid_variables_TBFC.registerTempTable("payload_Exception_Events_Fluid_variables_TBFC")

	val payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable = sqlContext.sql("SELECT  MachSer , TID , Bin , minDate ,  maxDate ,  min_SMU ,  max_payload  , Avg_Sum_FCR_Ltrs_per_Kms_Tonnes ,  Avg_Sum_FCR_LtrsperMins_Tonnes ,  Avg_No_of_Trips , CriticalPercent_Trend , CriticalPercent_Event , CriticalPercent_Fluid , CriticalHours , CriticalDays  , SMU_Event ,  Sum_endOvrSpd1 ,  Sum_engPreLubeOvr1 , Sum_grdLvlShut1 ,  Sum_brkRealted1 ,  Sum_hiOilTemp1 , Sum_hiFuelWtr1 ,  Sum_idlStndRely1 ,  Sum_payloadOvr1 , Sum_TotalAbuseEvents , fluidBadSampleCount , fluidSampleCount , fluidSampleSeverity ,sum_fluidAddedQnty, fluidChangedCount, filterChangedCount , flilterChangeRunningCount ,  BadSamplePercentage ,  ActualEngineHours, FluidBadSamplePerSMu  ,  FluidSampleSeverityScore ,  FilterChangePerHours ,  CumulativeFilterChangePerHours ,  Max_EngineIdleTime ,  Max_FuelConsumed , Max_EngineStartCount    FROM    payload_Exception_Events_Fluid_variables_TBFC LEFT OUTER JOIN cumulativeResult ON MachSer = Serial AND Bin = bin ") 


payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable.registerTempTable("payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable")




/*******************************************************
			Repair History calculations   TBBR

************************************************************************************************************************/
/*************************   class Records  ****************************************************************/



case class Histogram( Machine_hist : String , date_hist : java.sql.Date , SMU_hist : Double ) 

case class RepairDate ( Machine_Repair : String , date_repair : java.sql.Date , SMU_repair : Double )

case class Cumulative ( Machine_Cumulative : String , date_cumulative : java.sql.Date , SMU_cumulative : Double )

case class Events ( Machine_events : String , date_events : java.sql.Date , SMU_events : Double )

case class Payload( Machine_payload : String, date_payload : java.sql.Date , SMU_payload : Double )

case class Fluid ( Machine_fluid : String , date_fluid : java.sql.Date , SMU_Fluid : Double ) 

case class LagDateDiff( serial1 : String , date1 : Option[java.sql.Date] , lagDate1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] ) 

case class LagSMUDiff( serial2 : String , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] )

case class Temp1 ( serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double]  )

case class Temp2 ( serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double] , NULL_sno : Option[Double] , sno : Option[Double] )

case class Temp3 ( serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double] , NULL_sno : Option[Double] , sno : Option[Double] , L_SMU : Option[Double] , SMU_N : Option[ Double ]  )

case class Temp4(  serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double] , NULL_sno : Option[Double] , sno : Option[Double] , L_SMU : Option[Double] ,  SMU_N : Option[ Double ] , fillUp2 : Option[Double] )


case class Temp5(  serial1 : String , date1 : Option[java.sql.Date] , repair_SMU1 : Option[Double] , Max_SMU1 : Option[Double] , dayDiff1 : Option[Double] , date2 : Option[java.sql.Date] ,  lagDate2 : Option[java.sql.Date] , repair_SMU2 : Option[Double] , SMU : Option[Double] , lagSMU : Option[Double] , diffSMU : Option[Double] , dateDiff2 : Option[Double] , fillUp : Option[Double] , NULL_sno : Option[Double] , sno : Option[Double],L_SMU : Option[Double] ,  SMU_N : Option[ Double ] , fillUp2 : Option[Double] , New_SMU : Option[Double] )


case class Temp6 ( serial : String , date : java.sql.Date , SMU : Double , diff_Days : Option[Double] , diff_SMU : Double , bin : Int )




/**************************************************    MTBR calculatins *****************************************************/




     def dropHeader(data: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
         data.mapPartitionsWithIndex((idx, lines) => {
           if (idx == 0) {
             lines.drop(1)
           }
           lines
         })
       }


  
    def dayDiff ( d1 : Option[java.sql.Date] , d2 : Option[java.sql.Date] ) : Option[Double] = {
		
		if( d1.getOrElse(None) == None || d2.getOrElse( None ) == None ) None
		else
 Some( ( java.sql.Date.valueOf( d1.get.toString).getTime - java.sql.Date.valueOf(d2.get.toString).getTime )/ (1000.0 * 24 * 60 * 60 ) )

    }

   def getDouble( str : String ) : Option[Double] = {
		
		try  { Some( str.toDouble ) }
		catch { case e : Exception => None  }
    }

   def getString ( x  : Any ) : String = {
           try{ x.toString }
	   catch{ case e : Exception => None.toString }

  }

  
   def lagSqlDate( d : Any )  : Option[ java.sql.Date] = {

        try{  Some(java.sql.Date.valueOf( d.toString )) }
        catch{ case e : Exception => None}
    

     }
     
     
     def getSqlDate( str : String ) :  Option[java.sql.Date] = {
		try{
			Some( java.sql.Date.valueOf(str) )
		}
		catch{
			case e : Exception => None
		}
     }

   def max_SMU( a : Double , b : Double , c : Double , d : Double , e : Double  ) : Double = { 
	List( a , b , c , d , e  ).max
   }


		val histogramData = sc.textFile( "/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/TBR/hist.csv")
		val histogramHeader = histogramData.take( 1) ( 0 )
		val indexOfSerial_hist = histogramHeader.split(",").indexOf( "SERIAL_NUMBER")
		val indexOfVIMS_hist =  histogramHeader.split(",").indexOf( "VIMSTIME")
		val indexOfSMU_hist =  histogramHeader.split(",").indexOf( "SMU")

		val histogramWithOutHeader = dropHeader( histogramData ).filter( x => x.split(",")(indexOfVIMS_hist ).split(" ").length == 2 ) 

		val histogramSchemaRDD = histogramWithOutHeader.map( line => { val word = line.split( ",");  Histogram(word(indexOfSerial_hist), java.sql.Date.valueOf( word(indexOfVIMS_hist).split(" ")(0) ),word(indexOfSMU_hist).toDouble )} )

		histogramSchemaRDD.registerTempTable("HistogramTable")

		val histogramSummary1 = sqlContext.sql("SELECT Machine_hist , date_hist , max( SMU_hist ) SMUHist FROM  HistogramTable GROUP BY Machine_hist , date_hist " )
		

		histogramSummary1.persist()
		
		
		/******************************************* repaair History *******************************************************/
		
		
		val repairDates = sc.textFile( "/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/TBR/repair.csv")
		val repairDatesHeader = repairDates.take(1)(0)

		/* indexes of repairDate */
		val indexOfSerial_repair = repairDatesHeader.split(",").indexOf("Serial Number")
		val indexOfDate_repair = repairDatesHeader.split(",").indexOf("repairDate")
		val indexOfServiceMeter = repairDatesHeader.split(",").indexOf("serviceMeterMeasurement")

		/* droping Header of repair Date */
		val repairDatesWithOUtHeader = dropHeader( repairDates )

		val  repairDatesSchemaRDD = repairDatesWithOUtHeader.map( line => { val word = line.split(","); val smu_repair : Double = if( word(indexOfServiceMeter ).split(" ")(0).toDouble == 9999999  )  0.0  else  word(indexOfServiceMeter).split(" ")(0).toDouble; 
 RepairDate( word(indexOfSerial_repair) , java.sql.Date.valueOf( word(indexOfDate_repair) ) ,smu_repair  ) } )
 
		repairDatesSchemaRDD.registerTempTable("RepairDateTable")
 
		val repairDatesSummary = sqlContext.sql("SELECT Machine_Repair , date_repair , max(SMU_repair) SMURepair FROM  RepairDateTable GROUP BY  Machine_Repair , date_repair" ) 

		

		val cumulativeData = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/TBR/cumulative.csv" )
		val cumulativeHeader = cumulativeData.take(1)(0)
		
		val indexOfSerial_cumulative = cumulativeHeader.split(",").indexOf( "SERIAL_NUMBER" )
		val indexOfVIMS_cumulative = cumulativeHeader.split(",").indexOf( "VIMSTIME" )
		val indexOfSMU_cumulative = cumulativeHeader.split(",").indexOf("SMU")

		
		/* droping header  of cumulative */

		val cumulativeWithOutHeader = dropHeader( cumulativeData )

		val cumulativeSchemaRDD = cumulativeWithOutHeader.map( line => 
					{ 
						val word = line.split(",") 
					  Cumulative( word(indexOfSerial_cumulative), java.sql.Date.valueOf( word(indexOfVIMS_cumulative).split(" ")(0) ) , word(indexOfSMU_cumulative).toDouble ) } ).toSchemaRDD

		cumulativeSchemaRDD.registerTempTable( "CumulativeTable")

		val cumulativeSummary = sqlContext.sql("SELECT Machine_Cumulative , date_cumulative , max( SMU_cumulative ) SMUCumulative FROM CumulativeTable GROUP BY  Machine_Cumulative , date_cumulative ORDER BY Machine_Cumulative , date_cumulative ")


		/****************************************** Event Data ****************************************/
					/* Event Data  */
		val eventsData = sc.textFile( "/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/TBR/events.csv")

		/* events Header */

		val eventsHeader = eventsData.take(1)(0)

		/* indexes of events */

		val indexOfSerial_events = eventsHeader.split(",").indexOf( "MACH_SER_NO")
		val indexOfDate_events = eventsHeader.split(",").indexOf( "Date_Valid")
		val indexOfSMU_events = eventsHeader.split(",").indexOf( "Max_SMU")

		/* droping header of events */

		val eventsWithOutHeader = dropHeader( eventsData )

		val eventsSchemaRDD = eventsWithOutHeader.map( line => {
						val word = line.split(",") 
				 Events( word(indexOfSerial_events) , java.sql.Date.valueOf( word(indexOfDate_events) ) , if( word( indexOfSMU_events ) == "" ) 0.0 else word( indexOfSMU_events ).toDouble ) } ).distinct.toSchemaRDD

		

		
eventsSchemaRDD.registerTempTable( "EventsTable" )

		val eventsSummary = sqlContext.sql("SELECT   Machine_events , date_events , max( SMU_events ) SMUEvents FROM EventsTable GROUP BY Machine_events , date_events ORDER BY Machine_events , date_events  ")


		/************************* Payload Data ****************************/
					/* Payload Data */

		val payloadData = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/TBR/payload.csv")

		val payloadHeader = payloadData.take(1)(0)

		/* Payload indexes */

		val indexOfSerial_payload = payloadHeader.split(",").indexOf("MACH_SER_NO")
		val indexOfDate_payload = payloadHeader.split(",").indexOf("Date_Valid")
		val indexOfSMU_payload = payloadHeader.split(",").indexOf("SMU_payload")

		/* Payload Without Header */

		val payloadWithOutHeader = dropHeader( payloadData )

		val payloadSchemaRDD = payloadWithOutHeader.map( line => { 
				  val word = line.split(",")
			 Payload( word(indexOfSerial_payload) , java.sql.Date.valueOf( word(indexOfDate_payload) ) ,( if( word(indexOfSMU_payload) == "") 0.0 else word(indexOfSMU_payload).toDouble )  )  
			} )
		
			

		payloadSchemaRDD.registerTempTable("PayloadTable")		
		
		val payloadSummary = sqlContext.sql( "SELECT  Machine_payload , date_payload,  max( SMU_payload ) SMUPayload  FROM PayloadTable GROUP BY Machine_payload , date_payload  ORDER BY  Machine_payload , date_payload " )

	
      /*************************  Fluid Data *************************************************/
	
	val fluidData = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/TBR/fluid.csv")

	val fluidHeader = fluidData.take(1)(0)

	/* indexes of FluidData */

	val indexOfSer_Fluid = fluidHeader.split(",").indexOf("/sample/mfrSerialNumber")
	val indexOfDate_Fluid = fluidHeader.split(",").indexOf("Date")
	val indexOfServiceMeter_F =  fluidHeader.split(",").indexOf("/sample/serviceMeter")

	val fluidWithOutHeader = dropHeader( fluidData )

	val fluidSchemaRDD = fluidWithOutHeader.map( line => {val word = line.split(","); Fluid( word(indexOfSer_Fluid) , java.sql.Date.valueOf( word(indexOfDate_Fluid) ) , if( word(indexOfServiceMeter_F) == "" ) 0.0 else word(indexOfServiceMeter_F).toDouble   ) } )

	fluidSchemaRDD.registerTempTable( "FluidTable")

	val fluidSummaryTable = sqlContext.sql( "SELECT Machine_fluid , date_fluid , max( SMU_Fluid ) SMUFluid FROM FluidTable GROUP BY  Machine_fluid , date_fluid " )



/********************************  JOINS ***********************************************/

	repairDatesSummary.registerTempTable("RepairSummaryTable")

	cumulativeSummary.registerTempTable("CumalativeSummaryTable")

	val RJoinsC = sqlContext.sql( "SELECT  Machine_Repair , date_repair , SMURepair , SMUCumulative FROM RepairSummaryTable LEFT OUTER JOIN CumalativeSummaryTable ON Machine_Repair = Machine_Cumulative AND date_repair = date_cumulative " )

	RJoinsC.registerTempTable("RJoinsC")

	eventsSummary.registerTempTable( "eventsSummaryTable" )

	val RCJoinsE = sqlContext.sql( "SELECT  Machine_Repair , date_repair , SMURepair , SMUCumulative , SMUEvents  FROM RJoinsC LEFT OUTER JOIN eventsSummaryTable ON Machine_Repair = Machine_events AND date_repair = date_events " )

	RCJoinsE.registerTempTable("RCJoinsE")

	payloadSummary.registerTempTable("payloadSummaryTable")

	val RCEJoinsP = sqlContext.sql( "SELECT distinct Machine_Repair , date_repair , SMURepair , SMUCumulative , SMUEvents , SMUPayload  FROM RCJoinsE LEFT OUTER JOIN payloadSummaryTable ON Machine_Repair = Machine_payload AND date_repair = date_payload " )


	RCEJoinsP.registerTempTable("RCEJoinsP")

	fluidSummaryTable.registerTempTable("FluidSummaryTable")

	val RCEPJoinsF = sqlContext.sql( "SELECT distinct Machine_Repair , date_repair , SMURepair , SMUCumulative , SMUEvents , SMUPayload , SMUFluid FROM RCEJoinsP LEFT OUTER JOIN FluidSummaryTable ON Machine_Repair = Machine_fluid AND date_repair = date_fluid ")

	RCEPJoinsF.registerTempTable("RCEPJoinsF")

	histogramSummary1.registerTempTable( "histogramSummary1")

	val RCEPFJoinsH = sqlContext.sql( "SELECT  distinct Machine_Repair , date_repair , SMURepair , SMUCumulative , SMUEvents , SMUPayload , SMUFluid , SMUHist FROM  RCEPJoinsF LEFT OUTER JOIN histogramSummary1 ON Machine_Repair = Machine_hist AND date_repair = date_hist " )

	RCEPFJoinsH.registerTempTable("RCEPFJoinsH")


   
	sqlContext.registerFunction( "max_SMU" , max_SMU( _ : Double , _ : Double , _ : Double , _ : Double , _ : Double   ) )

	val maxSMURDD = sqlContext.sql( "SELECT  Machine_Repair , date_repair ,  SMURepair , max_SMU (  SMUCumulative , SMUEvents , SMUPayload , SMUFluid , SMUHist ) AS MAXSMU FROM RCEPFJoinsH ORDER BY Machine_Repair , date_repair, MAXSMU " )


	maxSMURDD.registerTempTable("maxSMURDD")

	val zippedMaxSMURDD = maxSMURDD.zipWithIndex

	val filteredSMU = zippedMaxSMURDD.map( x => x ) .filter( x => ! x._1.isNullAt( 3 ) )

	val lagMaxSMU =  filteredSMU.
			map( x => ( x._2 , x._1.mkString(",") ) ).
			flatMap { x => ( 0 to 1 ).
			     	  map( i => ( x._1 - i , x ) ) 
			} .map( x => ( x._1 , List( x._2  ) ) ).
			combineByKey( x => x , ( aggr: List[(Long,String)] , value ) => aggr ::: value  , ( aggr1 : List[(Long,String)] , aggr2 : List[(Long, String)] ) => aggr1 ::: aggr2 ).
			mapValues{ x => x.sortWith( ( x , y ) => x._1 < y._1 ) }.
			mapValues( x => {  if( x.length == 1 )
					      ( x(0)._1 , x(0)._2 + "," + x(0)._2.split(",")(3) + "," + x(0)._2.split(",")(1) )
					    else if (x(0)._2.split(",")(0) == x(1)._2.split(",")(0)  )
					       ( x(1)._1 , x(1)._2 + "," + x(0)._2.split(",")(3) + "," + x(0)._2.split(",")(1)  )
					     else 
						( x(1)._1 , x(1)._2 + "," + x(1)._2.split(",")(3) + "," + x(1)._2.split(",")(1) )
				         }).filter( x => x._1 != x._2._1 ).sortBy( _._1 )

		/****  optimized one  ******************************************************************

	

		val lagMaxSMU =  filteredSMU.
			map( x => ( x._2 , x._1.mkString(",") ) ).
			flatMap { x => ( 0 to 1 ).
			map( i => ( x._1 - i , x ) ) }.			
			combineByKey( x => List(x) , ( aggr: List[(Long,String)] , value ) => aggr ::: List(value)  , ( aggr1 : List[(Long,String)] , aggr2 : List[(Long, String)] ) => aggr1 ::: aggr2 ).
			mapValues{ x => x.sortWith( ( x , y ) => x._1 < y._1 ) }.
			mapValues( x => {  if( x.length == 1 )
					      ( x(0)._1 , x(0)._2 + "," + x(0)._2.split(",")(3) + "," + x(0)._2.split(",")(1) )
					    else if (x(0)._2.split(",")(0) == x(1)._2.split(",")(0)  )
					       ( x(1)._1 , x(1)._2 + "," + x(0)._2.split(",")(3) + "," + x(0)._2.split(",")(1)  )
					     else 
						( x(1)._1 , x(1)._2 + "," + x(1)._2.split(",")(3) + "," + x(1)._2.split(",")(1) )
				         }).sortBy( _._1 )


		*******************************************************************************************************/

	val lagSMUDate = lagMaxSMU.map( x => { 
			(   x._1 , 
				( 
					x._2._1 , 
					x._2._2 + "," + (
							  ( if ( ( x._2._2.split(",")(3).toDouble - x._2._2.split(",")(4).toDouble ) <= 0 )
								0.0.toString
							     else
							    (x._2._2.split(",")(3).toDouble - x._2._2.split(",")(4).toDouble ).toString
							   
                                                          ) + "," +  (
							
							( 
						            ( java.sql.Date.valueOf( x._2._2.split(",")(1) ).getTime - 
							      java.sql.Date.valueOf( x._2._2.split(",")(5) ).getTime ) / 
							      (1000 * 24 * 60 * 60 ) 
                                                            ).toString 
							 ) 
                                                     )  
                                ) 
                       ) 
               }  
           )



	/* 
		For Debugging the Code only
		lagSMUDate.filter( x => x._2._2.split(",")(6).toDouble == 0.0 )

	*/

	/* Other One  */
	val lagDate = zippedMaxSMURDD.
	      map( x => ( x._2 , (x._2 , x._1.mkString(",") ) ) ).
	      flatMap{ x => (0 to 1).map( i => ( x._1 - i , x._2 ) ) }.
	      combineByKey( value => List[(Long,String)]( value ) ,  ( aggr: List[(Long,String)] , value ) => aggr ::: List(value) , ( aggr1 : List[(Long,String)] ,  aggr2 : List[(Long,String)] ) => aggr1 ::: aggr2 ).
	      mapValues{ x => x.sortWith( (p,q) => p._1 < q._1 ) }.
	      mapValues( x => { 
				  if(x.length == 1){
				      ( x(0)._1 , x(0)._2 , None )
			          } 
				  else if( x(0)._2.split(",")(0) .equals ( x(1)._2.split(",")(0) )  ){
				     ( x(1)._1 , x(1)._2 ,  Some( java.sql.Date.valueOf( x(0)._2.split(",")(1) ) ) )
				  }
				  else {
				  ( x(1)._1 , x(1)._2 , None )
				  }
			      }
		        ).filter( x => x._1 != x._2._1 ).sortBy( _._1 )

	
		val lagDateDiff = lagDate.map( x => {   
				        val p = x._2._2.split(",")
					val date1 : Option[java.sql.Date] =   getSqlDate( p(1) )
					val date2 : Option[ java.sql.Date ] =  x._2._3
					if( ( date1 == None) || (date2 == None) ) {
                                           ( p(0) , date1 , date2 , getDouble( p(2 ) ) , getDouble( p(3) ), None  )
							
					}
					else{ 
                                ( p(0) , Some(java.sql.Date.valueOf( date1.get.toString )) , Some(java.sql.Date.valueOf(date2.get.toString) ) , getDouble( p(2 ) ) , getDouble( p(3) ), dayDiff( date1 , date2 )  )		
					}
			    	    }
			     )

/***************************  It Must be on left Side Table ********************* */


	val lagDateDiffSchema = lagDateDiff.map( x => LagDateDiff( x._1 , x._2 , x._3 , x._4 , x._5 , x._6) ).toSchemaRDD



/***** It is for Right Side Table (filtered Max removing Nulls) *************************/

val lagSMUDateMappedSchema = lagSMUDate.map( x => { 
					val p = x._2._2.split(",")
	    LagSMUDiff( 
                p(0) , 
		Some( java.sql.Date.valueOf( p(1) )  ) ,
                Some( java.sql.Date.valueOf( p(5) )  ) , 
                getDouble( p(2)) , 
                getDouble( p(3) ) , 
                getDouble( p(4) ) , 
                getDouble( p(6) ) , 
                getDouble( p(7) )  
              )   
	  }		
	).toSchemaRDD

/*****  Now Joining Left Table And Right Table **********************************/
     /*Left Table */
	lagDateDiffSchema.registerTempTable( "lagDateDiff" )
	     /* Right Table */
	lagSMUDateMappedSchema.registerTempTable("lagSMUDateMapped")

	val join1 = sqlContext.sql( "SELECT serial1, date1 , repair_SMU1 ,Max_SMU1 , dayDiff1 ,date2 , lagDate2 , repair_SMU2 , SMU , lagSMU , diffSMU ,dateDiff2    FROM lagDateDiff LEFT OUTER JOIN lagSMUDateMapped ON serial1 = serial2 AND date1 = date2 ORDER BY serial1 asc , date1 desc "  )


	join1.persist()

	
	val join1Zipped = join1.
		  zipWithIndex.
		  flatMap( x => 
			    {
			      ( 0 to 1 ).map( i => ( x._2 - i , ( x._2 , x._1 )  ) ) 
			    }  
			 ).
		  combineByKey( value => List( value ) , 
				( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value ) => aggr ::: List( value ) , 
	( aggr1 :  List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 
                  ).
		  mapValues( x => x.sortWith( ( p , q ) => p._1 < q._1 ) ) .
		  mapValues ( x => {
					if( x.length == 1 ){ 
					      if( x(0)._2.isNullAt( 10 ) ) {
					    ( x(0)._1 , x(0)._2(0) , x(0)._2(1) ,x(0)._2(2) ,x(0)._2(3) ,x(0)._2(4) ,x(0)._2(5) ,x(0)._2(6) ,x(0)._2(7) ,x(0)._2(8) ,x(0)._2(9) , x(0)._2(10) , x(0)._2(11) , 0.0   ) 
                                               }
					       else {  
 						( x(0)._1 , x(0)._2(0) , x(0)._2(1) ,x(0)._2(2) ,x(0)._2(3) ,x(0)._2(4) ,x(0)._2(5) ,x(0)._2(6) ,x(0)._2(7) ,x(0)._2(8) ,x(0)._2(9) , x(0)._2(10) , x(0)._2(11) , x(0)._2(10) ) 
			                        }
					 }
					else if( x(0)._2(0) == x(1)._2(0) ){
						if( x(1)._2.isNullAt( 10 ) ) {
					           ( x(1)._1 , x(1)._2(0) , x(1)._2(1) ,x(1)._2(2) ,x(1)._2(3) ,x(1)._2(4) ,x(1)._2(5) ,x(1)._2(6) ,x(1)._2(7) ,x(1)._2(8) ,x(1)._2(9) , x(1)._2(10) , x(1)._2(11), x(0)._2(10) ) 
						}
						else {
						    ( x(1)._1 , x(1)._2(0) , x(1)._2(1) ,x(1)._2(2) ,x(1)._2(3) ,x(1)._2(4) ,x(1)._2(5) ,x(1)._2(6) ,x(1)._2(7) ,x(1)._2(8) ,x(1)._2(9) , x(1)._2(10) , x(1)._2(11) , x(1)._2(10) ) 
						}
					}
					else {
					     if( x(1)._2.isNullAt( 10 ) ) {
						( x(1)._1 , x(1)._2(0) , x(1)._2(1) ,x(1)._2(2) ,x(1)._2(3) ,x(1)._2(4) ,x(1)._2(5) ,x(1)._2(6) ,x(1)._2(7) ,x(1)._2(8) ,x(1)._2(9) , x(1)._2(10) , x(1)._2(11), 0.0   )
						}
						else {
						    ( x(1)._1 , x(1)._2(0) , x(1)._2(1) ,x(1)._2(2) ,x(1)._2(3) ,x(1)._2(4) ,x(1)._2(5) ,x(1)._2(6) ,x(1)._2(7) ,x(1)._2(8) ,x(1)._2(9) , x(1)._2(10) , x(1)._2(11) , x(1)._2(10) ) 
		
						} 
					}		    
				    }).
				   filter( x => x._1 != x._2._1 ).sortBy( _._1 ).
				   map( x => ( getString ( x._2._2 )  , getString(x._2._3) , getString(x._2._4) ,getString( x._2._5) , getString(x._2._6) ,getString( x._2._7) , getString(x._2._8) , getString(x._2._9) , getString(x._2._10), getString(x._2._11) , getString(x._2._12) , getString( x._2._13) ,getString( x._2._14)  ) )


	

		val join1SMULag = join1Zipped.map( x => { Temp1( x._1 , getSqlDate( x._2) , getDouble( x._3 ) , getDouble( x._4 ) , getDouble( x._5 ) , getSqlDate( x._6 ) , getSqlDate( x._7 ) , getDouble( x._8 ) , getDouble( x._9 ) , getDouble ( x._10 ) , getDouble ( x._11 ), getDouble ( x._12 ) , getDouble ( x._13 )    ) } )

		join1SMULag.registerTempTable ( "join1SMULag" )


		val join1SMULagOrder = sqlContext.sql( "SELECT * FROM join1SMULag ORDER BY serial1 , date1 ")

		val join1Sno = join1SMULagOrder.map( x => ( x(0) , x ) ).
	       			combineByKey( value => List[(org.apache.spark.sql.Row)]( value ) ,  ( aggr : List[(org.apache.spark.sql.Row)] , value : org.apache.spark.sql.Row ) => aggr ::: List[(org.apache.spark.sql.Row)]( value ) , ( aggr1 : List[(org.apache.spark.sql.Row)] , aggr2 : List[(org.apache.spark.sql.Row)] ) => aggr1 ::: aggr2 ).
               mapValues( x => { 
			var sno = 0 
			var list : List[(org.apache.spark.sql.Row , Int)] = List[(org.apache.spark.sql.Row, Int)]()
			var temp : org.apache.spark.sql.Row = x(0)
			for ( p <- x ) {
			    if( p.isNullAt( 3) ) {
					if( sno != 0 ) 
				           list = list ::: List[(org.apache.spark.sql.Row , Int)]( (p , sno) )
					else 
					     list = list ::: List[(org.apache.spark.sql.Row , Int)]( (p , 1) )
			    }
			   else {
					 sno = sno + 1 
					 list = list ::: List[(org.apache.spark.sql.Row , Int)]( (p , sno) )
					 
				}
				
			}
	
                        list
                } ).
               flatMapValues( x => x ).
	       map( x => { 
			if( x._2._1.isNullAt( 3) )
			   (  x._2._1 , x._2._2 , x._2._2)
			else
			  (  x._2._1 , x._2._2 , 9999999) 
	}) .
	map( x => 
		(  getString ( x._1(0) ), 
		   getString(x._1(1)) ,
		   getString( x._1(2)) ,
		   getString( x._1(3)) , 
		   getString(x._1(4)) ,
		   getString( x._1(5)) ,
		   getString( x._1(6)),
                   getString( x._1(7) ),
                   getString( x._1(8)),
                   getString(x._1(9)) ,
                   getString( x._1(10)) ,
                   getString( x._1(11)) , 
                   getString(x._1(12)) ,
                   getString( x._2) , 
                   getString(x._3)  
                 ) 
           ) 


	val join1SnoSchema = join1Sno.map( x => 
			Temp2( x._1 , 
			       getSqlDate( x._2 ) , 
			       getDouble( x._3 ) ,  
                               getDouble( x._4 ) , 
                               getDouble( x._5 ) , 
                               getSqlDate( x._6 ) , 
                               getSqlDate( x._7 ) , 
                               getDouble( x._8 ) , 
                               getDouble( x._9 ) , 
                               getDouble( x._10 ) , 
                               getDouble( x._11 ) , 
                               getDouble( x._12 ) , 
                               getDouble( x._13 ) , 
                               getDouble( x._14 ) , 
                               getDouble( x._15 ) 
                             ) 
                       )



	join1SnoSchema.registerTempTable( "join1Sno" )

	val join1SnoSummary = sqlContext.sql( "SELECT serial1 , sno , sum(dateDiff2) sum_dayDiff  FROM join1Sno GROUP BY serial1 , sno ORDER BY serial1" )

	join1SnoSummary.registerTempTable( "join1SnoSummary" ) 

	val join2 = sqlContext.sql( "SELECT join1Sno.serial1 serial1, date1  , repair_SMU1 , Max_SMU1  , dayDiff1  , date2 ,  lagDate2  , repair_SMU2  , SMU  , lagSMU  , diffSMU  , dateDiff2 , fillUp  , NULL_sno  , join1Sno.sno sno, sum_dayDiff FROM join1Sno LEFT OUTER JOIN join1SnoSummary ON join1Sno.serial1 = join1SnoSummary.serial1 AND join1Sno.sno = join1SnoSummary.sno " )

	join2.registerTempTable( "join2")

	val join2f1 = sqlContext.sql( " SELECT  serial1, date1  , repair_SMU1 , Max_SMU1  , dayDiff1  , date2 ,  lagDate2  , repair_SMU2  , SMU  , lagSMU  , diffSMU  , dateDiff2 , fillUp  , NULL_sno  ,  sno, sum_dayDiff , IF( Max_SMU1 IS NULL , ( (fillUp / sum_dayDiff) * dateDiff2  ) , Max_SMU1  ) L_SMU FROM join2  ORDER BY serial1, date1 " )

	val join2SMU =   join2f1.zipWithIndex.map( x => ( x._1(0).toString , x) ).
		 combineByKey(  value => List[(org.apache.spark.sql.Row, Long)](value) , ( aggr : List[(org.apache.spark.sql.Row, Long)] , value  ) => aggr :::  List[(org.apache.spark.sql.Row, Long)]( value) , ( aggr1 : List[(org.apache.spark.sql.Row, Long)] , aggr2 : List[(org.apache.spark.sql.Row, Long)] ) => aggr1 ::: aggr2 ).
		mapValues( x => x.sortWith( (p,q) => p._2 < q._2 ) ).
		mapValues( x => {
				var list : List[(org.apache.spark.sql.Row , Double ) ] = List[(org.apache.spark.sql.Row , Double ) ]()
				var preSMU : Double = 0.0
				for( p <- x ) {
					if( p._1.isNullAt( 3 ) ) {
		list = list ::: List[(org.apache.spark.sql.Row , Double ) ] ( (p._1 , ( preSMU + getString(p._1(16)).toDouble  )) )
						preSMU = preSMU + getString(p._1(16)).toDouble
					}
					else{
	        list = list ::: List[(org.apache.spark.sql.Row , Double ) ] ( ( p._1 , getString(p._1(3)).toDouble) )
						preSMU = getString( p._1( 3 ) ).toDouble
					}
							
			
				}								

				list

			} ).
			flatMapValues( x => x ).
			map( x => x._2 ).
			map( x => {
			        (  getString(x._1(0)) ,
				   getString(x._1(1)) , 
				   getString(x._1(2)) ,
 				   getString(x._1(3)) , 
				   getString(x._1(4)) , 
				   getString(x._1(5)) , 
				   getString(x._1(6)) , 
				   getString(x._1(7)) , 
				   getString(x._1(8)) , 
				   getString(x._1(9)) ,
				   getString(x._1(10)) ,
				   getString(x._1(11)) , 
				   getString(x._1(12)) , 
				   getString(x._1(13)) , 
				   getString(x._1(14)) , 
				   getString(x._1(15)) ,
				   getString(x._1(16)) , 
				   x._2.toString   
                                 )				   			
			} )

	val join2f2 = join2SMU.map( x => {
				      Temp3(  
						x._1 , 
						getSqlDate( x._2 ) , 
						getDouble( x._3 ), 
						getDouble( x._4 ), 
						getDouble( x._5 ), 
						getSqlDate( x._6) , 
						getSqlDate( x._7 ) , 
						getDouble( x._8 ), 
						getDouble( x._9 ), 
						getDouble( x._10 ), 
						getDouble( x._11 ), 
						getDouble( x._12 ), 
						getDouble( x._13 ),
						getDouble( x._14 ),
						getDouble( x._15 ), 
						getDouble( x._16 ) , 
						getDouble( x._17 ) 
					)
				     }
			        )
	

	join2f2.registerTempTable( "join2f2" )

	val join2f3 = sqlContext.sql( "SELECT * FROM  join2f2 ORDER BY serial1 Asc , date1 Desc " )


	val join2f4 = join2f3.zipWithIndex.map( x => 
			     		( getString( x._1(0) ) , x ) 
			  		).
	  				combineByKey( value => List[(org.apache.spark.sql.Row , Long)](value) , 
  ( aggr : List[(org.apache.spark.sql.Row , Long)] , value ) => aggr ::: List[(org.apache.spark.sql.Row , Long)]( value ) , 
 ( aggr1 : List[(org.apache.spark.sql.Row , Long)] , aggr2 : List[(org.apache.spark.sql.Row , Long)] ) => aggr1 ::: aggr2 ).
 	
	mapValues( x => x.sortWith( (p,q) => p._2 < q._2 ) ).
	mapValues( x => 
		{
			var list : List[(org.apache.spark.sql.Row, Any) ] = List[(org.apache.spark.sql.Row, Any) ]()
			var preFill : Any = 0.0 
			for ( p <- x )  {
				if( p._1.isNullAt( 3 ) ) {
				list = list:::	List[(org.apache.spark.sql.Row, Any) ]( (p._1 , preFill) )
				}
				else{
				list = list ::: List[(org.apache.spark.sql.Row, Any) ]( (p._1 ,  p._1(11) ) ) 
				     preFill = getString(p._1(11))
				}
			}
		     list
		}).
		flatMapValues( x => x ).
		map( x => ( x._2 ) ).
		map( x =>  
			( 
				getString(x._1(0)) ,  
				getString(x._1(1)) , 
				getString(x._1(2)) ,
				getString(x._1(3)) , 
				getString(x._1(4)) , 
				getString(x._1(5)) , 
				getString(x._1(6)) , 
				getString(x._1(7)) , 
				getString(x._1(8)) , 
				getString(x._1(9)) , 
				getString(x._1(10)) ,
				getString(x._1(11)) , 
				getString(x._1(12)) , 
				getString(x._1(13)) , 
				getString(x._1(14)) , 
				getString(x._1(15)) ,   
				getString(x._1(16)) , 
				getString(x._2)   
			) 
		)


	
	val join2f5 = join2f4.map( x => 
			Temp4( 
				x._1 , 
				getSqlDate( x._2 ) , 
				getDouble( x._3 ), 
				getDouble( x._4 ), 
				getDouble( x._5 ), 
				getSqlDate( x._6) , 
				getSqlDate( x._7 ) , 
				getDouble( x._8 ), 
				getDouble( x._9 ), 
				getDouble( x._10 ), 
				getDouble( x._11 ), 
				getDouble( x._12 ), 
				getDouble( x._13 ),
				getDouble( x._14 ),
				getDouble( x._15 ), 
				getDouble( x._16 ) , 
				getDouble(x._17) , 
				getDouble( x._18) 
				) 
			)

	
	join2f5.registerTempTable( "join2f5")

	val join2f6 = sqlContext.sql("SELECT * FROM join2f5 ORDER BY serial1 , date1 ")

	val join2f7 = join2f6.zipWithIndex.
	      map( x => ( x._1(0) , ( x._1 , x._2 ) ) ).
	      combineByKey(  
		( value : (org.apache.spark.sql.Row, Long) ) => List[(org.apache.spark.sql.Row, Long)] ( value ) ,
  (  aggr: List[(org.apache.spark.sql.Row, Long)] , value : (org.apache.spark.sql.Row, Long)  ) =>  aggr ::: List[(org.apache.spark.sql.Row, Long)] (value) , ( aggr1 : List[(org.apache.spark.sql.Row, Long)] , aggr2 : List[(org.apache.spark.sql.Row, Long)] ) => aggr1 ::: aggr2 ).
		
		mapValues( x => x.sortWith( (p,q) => p._2 < q._2 ) ).
		mapValues( x => {
			var list : List[(org.apache.spark.sql.Row, Option[Double] )] = List[(org.apache.spark.sql.Row, Option[Double])]()
			var prevSMU : Option[Double] = Some(0.0)
			var temp : Option[Double] = None
			for ( p <- x ){
				if( p._1.isNullAt(3) ){
					
					if( p == x(0) ){
						list = list ::: List[(org.apache.spark.sql.Row, Option[Double])]( (p._1, prevSMU ) )
					 }
					else{
						try{
	temp =  Some(( ( p._1(12).toString.toDouble / p._1(17).toString.toDouble ) * p._1(4).toString.toDouble ) + prevSMU.getOrElse( 0.0))
						}
						catch{
					case e : Exception => temp = None							
						}
	list = list :::  List[(org.apache.spark.sql.Row, Option[Double])] ( ( p._1 , temp  ) )
					}
				}
				else{
         list = list ::: List[(org.apache.spark.sql.Row, Option[Double])]( ( p._1 , Some(p._1( 3).toString.toDouble  )) 	)			  
 
				}
			} 
			list
		}).
		flatMapValues( x => x ).
		map( x => x._2 ).
		map( x => {
			(   	getString(x._1(0) ) , 
				getString(x._1(1) ) , 
				getString(x._1(2) ) , 
				getString(x._1(3) ) , 
				getString(x._1(4) ) , 
				getString(x._1(5) ) , 
				getString(x._1(6) ) , 
				getString(x._1(7) ) , 
				getString(x._1(8) ) , 
				getString(x._1(9) ) , 
				getString(x._1(10) ) , 
				getString(x._1(11) ) , 
				getString(x._1(12) ) , 
				getString(x._1(13) ) , 
				getString(x._1(14) ) , 
				getString(x._1(15) ) , 
				getString(x._1(16) ) ,
				getString(x._1(17) ) , 
				x._2.getOrElse( None ).toString	 

                 	)     
			}
		   )

	val join2f8 = join2f7.map( x => 
				Temp5(   
   					getString( x._1 ) , 
					getSqlDate( x._2 ) , 
					getDouble( x._3 ) , 
					getDouble( x._4 ) , 
					getDouble( x._5) , 
					getSqlDate( x._6) , 
					getSqlDate( x._7) , 
					getDouble( x._8) , 
					getDouble( x._9) ,  
					getDouble( x._10) , 
					getDouble( x._11) ,
					getDouble( x._12) , 
					getDouble( x._13) ,
					getDouble( x._14) , 
					getDouble( x._15) ,
					 getDouble( x._16) , 
					getDouble( x._17) , 
					getDouble( x._18) ,
					getDouble( x._19)             
				    )
				)

	join2f8.registerTempTable( "join2f8")

	val join2f9 = sqlContext.sql( "SELECT distinct serial1 , date1 ,New_SMU FROM join2f8 WHERE New_SMU IS NOT NULL  ORDER BY  serial1 , date1 " )	


	
	val join2F1 = join2f9. zipWithIndex.map( x => 
		( x._1(0) , x ) ).
		combineByKey( ( value : (org.apache.spark.sql.Row, Long) ) => List[(org.apache.spark.sql.Row, Long)] ( value ) ,
  (  aggr: List[(org.apache.spark.sql.Row, Long)] , value : (org.apache.spark.sql.Row, Long)  ) =>  aggr ::: List[(org.apache.spark.sql.Row, Long)] (value) , ( aggr1 : List[(org.apache.spark.sql.Row, Long)] , aggr2 : List[(org.apache.spark.sql.Row, Long)] ) => aggr1 ::: aggr2 

		).
		
		mapValues( x => x.sortWith((p,q) => p._2 < q._2 ) ).
		mapValues( x => {
					var preSMU : Double = 0.0 
					var preDate : Option[java.sql.Date]  = None
 					var list : List[(org.apache.spark.sql.Row, Option[java.sql.Date] , Double ) ] = List[(org.apache.spark.sql.Row, Option[java.sql.Date] , Double ) ] ()
				
					for( p <- x ) {

      list = list ::: List[(org.apache.spark.sql.Row, Option[java.sql.Date] , Double ) ] ( 
									(    p._1 , 
	     								     preDate ,
	     								     preSMU 
									)
								    )
		  			 preDate = lagSqlDate( p._1(1) )
		   			 preSMU =  p._1( 2).toString.toDouble
    				       }

  				list
		           } ).
	 	flatMapValues( x => x ).
	 	map( x => x._2 ).
         	map( x => 
			( x._1(0).toString ,  getString ( x._1(1) ) , getString( x._1( 2 ) ) , x._2 , x._3 ) 
		   )

		 


	val   join2F2  = join2F1.
		      map( x => {
	     				( x._1 , java.sql.Date.valueOf( x._2 ) , x._3.toDouble , x._4 , x._5     )
				} ).
    	                 map( x => {
				Temp6( x._1 , x._2 , x._3 , dayDiff(  Some( x._2 ) , x._4 ) , x._3 - x._5  ,  ( Math.floor( x._3 / 100 ).toString.split("\\.")(0).toInt  * 100 ) + 100       )  
		
			})

     
	join2F2.registerTempTable( "join2F2" )

    	val lagSMUDateFinal =  sqlContext.sql( "SELECT serial , date , SMU , diff_Days , diff_SMU , bin , IF( diff_SMU <= 0 , (diff_Days * 22 ), diff_SMU ) TBR_Hour     FROM join2F2 ORDER BY serial , date ")

    

     	lagSMUDateFinal.registerTempTable( "lagSMUDateFinal")

    	val table_1 = sqlContext.sql( "SELECT  serial , date , SMU , diff_Days , diff_SMU , bin , TBR_Hour , IF(diff_Days IS NULL , TBR_Hour /22 , diff_Days  ) TBR_InDays FROM lagSMUDateFinal ORDER BY serial , date " )
     
     	table_1.registerTempTable( "table_1" ) 
		
     	lagSMUDateFinal.registerTempTable( "lagSMUDateFinal")
    
   	join1.registerTempTable( "join1")
    
    	val table_2  = sqlContext.sql(  " SELECT serial1 , date1 , count( date1 )  count_  FROM  join1 GROUP BY serial1 , date1 " )    
         
     	table_2.registerTempTable( "table_2" )
  	
	val result = sqlContext.sql("SELECT serial , date , SMU , diff_Days , diff_SMU , bin , TBR_Hour , TBR_InDays , count_  FROM table_1 JOIN table_2 ON serial = serial1 AND date = date1 ")

	 result.registerTempTable( "resultTable")
	 
	sqlContext.sql("SELECT serial , date , SMU , TBR_Hour , TBR_InDays , bin , count_  As No_Repairs FROM  resultTable " ).map( x => x.mkString(",") ) 

	def getBin_MTBR( x : Any ) : Option[ Double ] = {
		try {
			Some(( Math.floor ( x.toString.toDouble / 100 ) * 100 ) + 100)  
		}		
		catch {
			 case e : java.lang.NullPointerException => None
			case p : java.lang.Exception => None
		}	
	}

	sqlContext.registerFunction("getBin_MTBR" , getBin_MTBR( _ : Any ) )
		
	val MTBR_result = sqlContext.sql("SELECT serial , date , SMU , diff_Days , diff_SMU , getBin_MTBR( bin ) AS Bin_MTBR , TBR_Hour , TBR_InDays , count_ AS No_Of_Repairs  FROM table_1 JOIN table_2 ON serial = serial1 AND date = date1  ")

	 MTBR_result.registerTempTable( "MTBR_result")
	 
/***********************************************************************************************************************************	

						MTTR calculations    
************************************************************************************************************************************/
case class MachineDates_TTR( SerialNumber : String , date : java.sql.Date )

case class RepairHistoryData( Serial : String , seriiceMeterMeasurement : Option[Double] , comment : String , dtCodeDescription : String , fcodeDescription : String , repairDate : java.sql.Date  )


case class PostPayload( MACH_SER_NO : String , Date_Valid : java.sql.Date , SMU : Option[Double] )

case class LaggedPayLoad( MACH_SER_NO  : Option[String] , Date_Valid : Option[java.sql.Date] , min_Payload_SMU : Option[Double] ,  max_Payload_SMU : Option[Double] , lag_max_smu : Option[Double] , lagDate : Option[java.sql.Date] , GapInDays : Option[Long] , GapInDays_24 : Option[Double]    )


case class Join2Class ( SerialNumber : Option[String], date: Option[java.sql.Date] , seriiceMeterMeasurement: Option[Double]  ,dtCodeDescription : Option[String] , fcodeDescription:  Option[String] , comment :  Option[String] , repairDate : Option[java.sql.Date]  , Date_Valid : Option[java.sql.Date] ,  min_Payload_SMU  : Option[Double],max_Payload_SMU : Option[Double], lag_max_smu : Option[Double], lagDate: Option[java.sql.Date] , GapInHours : Option[Double], GapInDays: Option[Double],  GapInDays_24: Option[Double] , DummyDate : Option[java.sql.Date])


case class LagCalculations (  Machine : Option[String], Date : Option[java.sql.Date],  DownDate : Option[java.sql.Date], UpDate : Option[java.sql.Date], UpTimeSMU :  Option[Double], DownTimeSMU : Option[Double], DayDifference : Option[Double], SMUDifference : Option[Double] ) 

case class lagDownTimeSMUClass ( Machine_TTR : Option[String] , DownDate : Option[java.sql.Date] , upDate : Option[ java.sql.Date] , UpTimeSMU : Option[Double] , DownTimeSMU : Option[Double] , DayDiff : Option[Double] , SMUDiff : Option[Double] , Ratio : Option[Double] , TTR_inDays : Option[Double] , laggedTimeSMU : Option[Double] , TimeInOperation : Option[Double]  )



def dropHeader(data: org.apache.spark.rdd.RDD[String]): org.apache.spark.rdd.RDD[String] = {
         data.mapPartitionsWithIndex((idx, lines) => {
           if (idx == 0) {
             lines.drop(1)
           }
           lines
         })
       }

def getDouble( str : Any ) : Option[Double] = {
	try {
		Some( str.toString.toDouble )
	}
	catch { case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None }
}

def generateDates(startDateString : String , endDateString : String  ): List[java.sql.Date ] = {
	//val startDate = java.sql.Date.valueOf( 	startDateString )
	//val endDate = java.sql.Date.valueOf( endDateString )
	
	val sqlStartDate = java.sql.Date.valueOf( startDateString )
	val sqlEndDate = java.sql.Date.valueOf( endDateString )
	var dateList = List[java.sql.Date]( sqlStartDate )
	val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
	val c = java.util.Calendar.getInstance()
	   c.setTime( sqlStartDate )
	   c.add( java.util.Calendar.DATE , 1 )
	 var dt = sdf.format(c.getTime())
	 while( java.sql.Date.valueOf(dt)  before sqlEndDate ) {
		 dateList = dateList ::: List[java.sql.Date]( java.sql.Date.valueOf(dt) )
		 c.add( java.util.Calendar.DATE , 1 )
		 dt = sdf.format(c.getTime())
	 } 
	dateList ::: List[java.sql.Date](java.sql.Date.valueOf(dt) )
}

def getSqlDate(str : Any ): Option[java.sql.Date] = {
	try{ Some(java.sql.Date.valueOf(str.toString)) }
	catch{ 
		case e : java.lang.Exception => None 
		case p : java.lang.NullPointerException => None		
		}
}

def getDifference ( x1 : Any , x2 : Any ) : Option[Double] = {
	
	try{ Some( x1.toString.toDouble - x2.toString.toDouble ) }
	catch { 
		case e: java.lang.Exception => None
		case p : java.lang.NullPointerException => None 
	}
}

def dayDiff( d1 : Any , d2 : Any ) : Option[Long] = {
	try{
	Some( (java.sql.Date.valueOf( d1.toString ) .getTime - java.sql.Date.valueOf( d2.toString ) .getTime ) / (1000 * 24 * 60 * 60))
		
	}
	catch{ 
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None 
	}
}

def getString ( str : Any ) : Option[String] = {
	try { Some(str.toString)}
	catch{ 
	case p : java.lang.NullPointerException => None
	case e : java.lang.Exception => None }
}





val repairHistory = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/TTR/RepairHistoryWhaleback.csv")

val repairHistoryHeader = repairHistory.take( 1) (0).split(",")

val indexOfSerial = repairHistoryHeader.indexOf("Serial Number")
val indexOfServiceMeterMeasurement = repairHistoryHeader.indexOf("serviceMeterMeasurement")
val indexOfComment = repairHistoryHeader.indexOf("comment")
val indexOfdtCodeDescription =  repairHistoryHeader.indexOf("dtCodeDescription")
val indexOffcodeDescription = repairHistoryHeader.indexOf("fcodeDescription")
val indexOfrepairDate = repairHistoryHeader.indexOf("repairDate")

val filteredRepairHistory = dropHeader(repairHistory)

/*
  generateDates(startdate, enddate ) generates dates so need of file 
 val MachineDatesData = dropHeader( sc.textFile("/home/amjath/Sriky/TTRCalculationsERC/TTRData/MachineDates.csv") )

*/



val mappedRepairHistory =  filteredRepairHistory.map( x => { val words = x.split(",")
							
	RepairHistoryData( words(indexOfSerial) , getDouble( words(indexOfServiceMeterMeasurement).split(" ")(0) ) , words(indexOfComment), words(indexOfdtCodeDescription) , words(indexOffcodeDescription) , java.sql.Date.valueOf(words(indexOfrepairDate))  )		
			 } )



val mappedMachineDates = filteredRepairHistory.map( x => x.split(",")( indexOfSerial ) ).distinct.map( x => ( x , generateDates("2011-01-01" , "2016-06-01")  ) ).flatMapValues( x => x ).map( x => MachineDates_TTR( x._1 , x._2 ) )
 

mappedRepairHistory.registerTempTable("mappedRepairHistory")

mappedMachineDates.registerTempTable("mappedMachineDates")


val join1_TTR = sqlContext.sql("SELECT SerialNumber , date , seriiceMeterMeasurement , comment ,dtCodeDescription , fcodeDescription , repairDate  FROM mappedMachineDates LEFT OUTER JOIN mappedRepairHistory ON SerialNumber = Serial AND date = repairDate ")

join1_TTR.registerTempTable("join1_TTR")


/**  PostPayload loading file ***/


val postPayload = sc.textFile("/home/amjath/Sriky/ERC all Data/Inputdata And Outputs/TTR/PostPayload.csv")

val postPayloadHeader = postPayload.take(1)(0).split(",")

val indexOfMach = postPayloadHeader.indexOf("MACH_SER_NO")
val indexOfDate = postPayloadHeader.indexOf("Date_Valid")
val indexOfSMU_payload = postPayloadHeader.indexOf("SMU_payload")

val postPayloadData = dropHeader(postPayload)



val mappedPayload = postPayloadData.map ( x => { val words = x.split(",")
				
PostPayload( words(indexOfMach) , java.sql.Date.valueOf( words( indexOfDate)) , getDouble(words(indexOfSMU_payload) ) )				
				  }  )

mappedPayload.registerTempTable("mappedPayload")


val filteredMappedPayload = sqlContext.sql("SELECT  distinct MACH_SER_NO , Date_Valid ,SMU  FROM mappedPayload WHERE Date_Valid >= '2011-01-01' ")

filteredMappedPayload.registerTempTable( "filteredMappedPayload")

val payLoad = sqlContext.sql("SELECT MACH_SER_NO , Date_Valid , min(SMU) AS min_Payload_SMU , max(SMU) As max_Payload_SMU  FROM filteredMappedPayload GROUP BY  MACH_SER_NO,Date_Valid  ORDER BY MACH_SER_NO,Date_Valid")



val laggedPayLoad = payLoad.zipWithIndex. flatMap( x => 
			    {
			      ( 0 to 1 ).map( i => ( x._2 - i , ( x._2 , x._1 )  ) ) 
			    }  
			 ).
		  combineByKey( value => List( value ) , 
				( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value ) => aggr ::: List( value ) , 
	( aggr1 :  List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 
                  ). 
		mapValues( x => x.sortWith( ( p , q ) => p._1 < q._1 ) ).
		mapValues( x => { 
			if(x.length == 1){
				
		( x(0) , getDouble(x(0)._2(3)), getSqlDate( x(0)._2(1) ) , Some(0.toString.toLong), Some(0.0)     )			
			}		
			else if( x(0)._2(0).toString == x(1)._2(0).toString ) {
				
  ( x(1) ,  getDouble( x(0)._2(3) ) , getSqlDate( x(0)._2(1)  ) , dayDiff( x(1)._2(1) , x(0)._2(1) ) , try {Some( dayDiff( x(1)._2(1) , x(0)._2(1) ).getOrElse(null).toString.toDouble * 24 )   } catch { case e :  java.lang.Exception => None ; case p : java.lang.NullPointerException => None }    )				 
			
			}
			else {
				
				( x(1) , getDouble( x(1)._2(3) )  ,  getSqlDate( x(1)._2(1) ), Some( 0.toString.toLong), Some(0.0)  )
			}
} ).filter( x => x._1 != x._2._1._1 ).map( x => ( x._2._1._2 , x._2._2 , x._2._3 , x._2._4 , x._2._5 ) )





val payloadAndDateLag = laggedPayLoad.map( x => LaggedPayLoad( getString(x._1(0)), getSqlDate( x._1(1) ) , getDouble( x._1(2) ) , getDouble(x._1(3) ) , x._2 , x._3 ,x._4,x._5  ) )

payloadAndDateLag.registerTempTable("payloadAndDateLag")

val payloadAndDateformala = sqlContext.sql("SELECT MACH_SER_NO , Date_Valid , min_Payload_SMU , max_Payload_SMU , lag_max_smu , lagDate , min_Payload_SMU - lag_max_smu AS GapInHours ,GapInDays , GapInDays_24  FROM  payloadAndDateLag ")

payloadAndDateformala.registerTempTable("payloadAndDateformala")




val join2_TTR = sqlContext.sql("SELECT SerialNumber , date , seriiceMeterMeasurement  ,dtCodeDescription , fcodeDescription , comment, repairDate , Date_Valid,  min_Payload_SMU ,max_Payload_SMU , lag_max_smu , lagDate , GapInHours, GapInDays,  GapInDays_24   FROM join1_TTR LEFT OUTER JOIN payloadAndDateformala ON  SerialNumber = MACH_SER_NO  AND date = Date_Valid  ")





val join2exp1 = join2_TTR.map( x => { if(! x.isNullAt(10) ) 
			( x , getSqlDate( x(1) ) ) 
		  else 
			( x , None )  
		} ).map( x => { 
			Join2Class( getString( x._1(0) ) , getSqlDate( x._1(1) ) , getDouble( x._1(2)) , getString( x._1(3) ),getString( x._1(4) ), getString( x._1(5) ), getSqlDate(x._1(6) ) ,getSqlDate(x._1(7) ) , getDouble( x._1(8)) , getDouble( x._1(9) ) , getDouble(x._1(10) ) , getSqlDate( x._1(11) ) , getDouble( x._1(12)) , getDouble( x._1(13)) , getDouble( x._1(14)) , x._2 )		
		} )


join2exp1.registerTempTable("join2exp1")


val join1exp2 = sqlContext.sql("SELECT SerialNumber , date , seriiceMeterMeasurement  ,dtCodeDescription , fcodeDescription , comment, repairDate , Date_Valid,  min_Payload_SMU ,max_Payload_SMU , lag_max_smu , lagDate , GapInHours, GapInDays,  GapInDays_24 , DummyDate  FROM join2exp1  ORDER BY  SerialNumber asc, date desc  ")


/* Lags Of  DownDate, UpDate, UpTimeSMU , DownTimeSMU    */

val lagsCalculated1 = join1exp2.zipWithIndex.map( x => (x._1(0).toString , (x._1 , x._2  ) ) ).
				combineByKey( 
					value  => List[(org.apache.spark.sql.Row, Long )]( value) , 	
	   ( aggr : List[(org.apache.spark.sql.Row, Long )]   , value  ) => aggr ::: List[(org.apache.spark.sql.Row, Long ) ] (value) , ( aggr1 :  List[(org.apache.spark.sql.Row, Long ) ] , aggr2 : List[(org.apache.spark.sql.Row, Long ) ] ) => aggr1 ::: aggr2  ) .
 mapValues( x => x.sortWith( (p,q) => p._2 < q._2  ) ) .

mapValues( x => { 

var list : List[ ( org.apache.spark.sql.Row , Option[java.sql.Date], Option[java.sql.Date] ,Option[Double] ,  Option[Double] ,   Long ) ] =  List[ ( org.apache.spark.sql.Row , Option[java.sql.Date] ,Option[java.sql.Date], Option[Double] ,  Option[Double] ,   Long ) ] ()

	var prevLagDate :   Option[java.sql.Date]   =   None 
	var preLagDummyDate : Option[java.sql.Date] = None
	var preMinPayload : Option[Double] = Some( 0.0 )
	var preMaxPayload : Option[Double] = Some( 0.0 )

for( p <- x ) {  
	
	list = list ::: List[ ( org.apache.spark.sql.Row ,Option[java.sql.Date], Option[java.sql.Date] , Option[Double] , Option[Double] ,   Long ) ] ( (p._1,  if( p._1.isNullAt(11 )) prevLagDate else getSqlDate( p._1(11) ) , if( p._1.isNullAt(15 ) ) preLagDummyDate else getSqlDate( p._1(15) ) , if( p._1.isNullAt(8 ) ) preMinPayload else getDouble( p._1(8) ), if( p._1.isNullAt(10 ) ) preMaxPayload else getDouble( p._1(10) ) ,  p._2   ))
	
	prevLagDate =   if( p._1.isNullAt(11 ) ) prevLagDate else getSqlDate( p._1(11) )   	
	preLagDummyDate = if( p._1.isNullAt(15 ) ) preLagDummyDate else getSqlDate( p._1(15) )
	
	preMinPayload = if( p._1.isNullAt(8 ) ) preMinPayload else getDouble( p._1(8) )   	
	preMaxPayload = if( p._1.isNullAt(10 ) ) preMaxPayload else getDouble( p._1(10) )
}

list 

} ).flatMapValues( x => x ).map( _._2 )
.filter( x => ! x._1.isNullAt(4) )   /* filtering fcodeDescriptions NULL */





/* SerialNumber , date , fcodeDescription , DownDate, UpDate, UpTimeSMU , DownTimeSMU , DayDifference, SMUDifference */



/* SerialNumber , date , fcodeDescription , DownDate, UpDate, UpTimeSMU , DownTimeSMU , DayDifference, SMUDifference */

val mappedLagCalculations = lagsCalculated1.map( x => LagCalculations ( getString(x._1(0)) , getSqlDate( x._1(1) ) , x._2 , x._3 , x._4 , x._5 , getDouble(dayDiff(  x._3.getOrElse("null") , x._2.getOrElse("null") ).getOrElse("null")) , getDouble( getDifference( x._4.getOrElse("null"), x._5.getOrElse("null") ).getOrElse("null") )   ) )
 
mappedLagCalculations.registerTempTable("mappedLagCalculationsTable")

def Ratio( x : Any , y : Any ) : Option[Double] = { 
	try{ if( y.toString.toDouble == 0.0 ) None else Some( x.toString.toDouble / y.toString.toDouble) }
	catch{ case e : java.lang.Exception => None }
}

def ttr( x : Any ) : Option[ Double ] = {
	try {  if( x.toString.toDouble > 30)  Some(0.0) else   Some( x.toString.toDouble ) }
	catch {case e : java.lang.Exception => None ; 
           
		 } 
 }

sqlContext.registerFunction("ttr" , ttr( _ : Any) )

sqlContext.registerFunction("Ratio" , Ratio( _ : Any , _ : Any) )

val TTRCalculated = sqlContext.sql("SELECT distinct  Machine , DownDate , UpDate ,UpTimeSMU ,DownTimeSMU , DayDifference , SMUDifference ,  DayDifference / SMUDifference  AS ratio , ttr( DayDifference ) AS TTRInDays  FROM mappedLagCalculationsTable ORDER BY Machine , DownDate  " )


val lagDownTimeSMU = TTRCalculated.zipWithIndex.
		  flatMap( x => 
			    {
			      ( 0 to 1 ).map( i => ( x._2 - i , ( x._2 , x._1 )  ) ) 
			    }  
			 ).
		  combineByKey( value => List( value ) , 
				( aggr : List[(Long , org.apache.spark.sql.Row ) ] , value ) => aggr ::: List( value ) , 
	( aggr1 :  List[(Long , org.apache.spark.sql.Row ) ] , aggr2 : List[(Long , org.apache.spark.sql.Row ) ] ) => aggr1 ::: aggr2 
                  ).
		  mapValues( x => x.sortWith( ( p , q ) => p._1 < q._1 ) ).
		  mapValues( x => 
				if( x.length == 1 ){
					(x(0) , 0.0 ) 
				}
				else if( x(0)._2(0) == x(1)._2(0) ){
					
					( x(1) , x(0)._2(4) )
				}
				else {
					( x(1) , 0.0 )
				}
		   ).filter( x => x._1 != x._2._1._1 ).
		map( x => ( x._2._1._2 , x._2._2) ).
		map( x => ( getString( x._1(0) ) , getSqlDate( x._1(1) ) , getSqlDate( x._1(2) ) , getDouble( x._1(3) ) , getDouble( x._1(4) ) , getDouble( x._1(5) ) , getDouble( x._1(6)) , getDouble( x._1(7)) , getDouble( x._1(8)) , getDouble( x._2 ) , getDifference( x._1(4) , x._2  )   ) ) 



/*
 Machine , DownDate , UpDate ,UpTimeSMU ,DownTimeSMU , DayDifference , SMUDifference ,  DayDifference / SMUDifference  AS ratio , ttr( DayDifference ) AS TTRInDays , laggedDownTimeSMu , TimeInOperation
*/



val mappedLaggedDownTimeSMU = lagDownTimeSMU.map( x => lagDownTimeSMUClass( x._1 , x._2 , x._3 , x._4 , x._5 , x._6 , x._7 , x._8 , x._9 , x._10 , x._11 ) )

mappedLaggedDownTimeSMU.registerTempTable("DownTimeSMUTable" )

def TTRperTimeOperationInHours ( TTRinDays : Any ,  TTRinOperation : Any ) : Option[Double] = {
			
	try {	
			val x = (TTRinDays.toString.toDouble * 24) /  TTRinOperation.toString.toDouble
			if( x >= 10 ) Some(0.0) else Some( x )
	  }
	catch{ case e: java.lang.Exception => None  ; case p : java.lang.NullPointerException => None }
} 

def getBin_TTR( x : Any  ) : Option[Double] = {
	try { Some( (Math.floor(x.toString.toDouble /100) * 100 ) + 100 ) }
	catch { case e : java.lang.Exception => None ; case p : java.lang.NullPointerException => None }
}

sqlContext.registerFunction( "getBin_TTR" ,  getBin_TTR ( _ : Any ) )

sqlContext.registerFunction("TTRperTimeOperationInHours" , TTRperTimeOperationInHours(_ : Any , _ : Any))

val mappedLaggedDownTimeSMUCalculations = sqlContext.sql("SELECT Machine_TTR , DownDate ,  upDate , UpTimeSMU , DownTimeSMU , DayDiff , SMUDiff , Ratio , TTR_inDays ,laggedTimeSMU, TimeInOperation , TTRperTimeOperationInHours(TTR_inDays, TimeInOperation ) AS TTR_Operation_Hours , getBin_TTR( DownTimeSMU ) AS Bin_TTR   FROM DownTimeSMUTable  ORDER BY Machine_TTR , DownDate ")



//Machine_TTR , DownDate ,  upDate , UpTimeSMU , DownTimeSMU , DayDiff , SMUDiff , Ratio , TTR_inDays ,laggedTimeSMU, TimeInOperation , TTRperTimeOperationInHours(TTR_inDays, TimeInOperation ) AS TTR_Operation_Hours , getBin_TTR( DownTimeSMU ) AS Bin_TTR   FROM DownTimeSMUTable 

mappedLaggedDownTimeSMUCalculations.registerTempTable("mappedLaggedDownTimeSMUCalculations")


val TTRERC = sqlContext.sql("SELECT Machine_TTR , DownDate , TTR_inDays , TimeInOperation , TTR_Operation_Hours , Bin_TTR     FROM mappedLaggedDownTimeSMUCalculations ")

TTRERC.registerTempTable("TTRERCTable")
















/******************************************************************************************************************************

                        Post TTR        MTTR calculated


val payloadSummary_formaula = sqlContext.sql("SELECT Machine_Ser ,StartDate  , EndDate , DateValid  , SMU_payload , No_of_Trips ,Avg_PAYLOADWEIGHT,Sum_PAYLOADWEIGHT, Sum_LOADTIME ,sum_EMTYSTOPTIME , Sum_EMPTYTRAVELTIME ,Sum_EMPTYTRAVELDIST , Sum_LOADSTOPTIME , Sum_LOADTRAVELTIME ,Sum_LOADTRAVELDIST ,Sum_LOADSTOPTIME ,Sum_LOADTRAVELTIME , Sum_LOADTRAVELDIST ,Sum_LOADERPASSCOUNT ,  Sum_FUELSUSEDINCYCLE , Sum_SHIFTCOUNT ,  Sum_CARRYBACK , Sum_FCR_Ltrs_per_Kms_Tonnes,  Sum_FCR_LtrsperMins_Tonnes, Sum_Overload_count_1_1 , sum_Overload_1_1 ,Sum_Overload_1_2_count , Sum_Overload_1_2 , Sum_TripTime , Sum_Pyld_LdTrvSpd , Sum_FCR_LTRS_MINS , Avg_FCR_Ltrs_per_Kms_Tonnes , Avg_FCR_LtrsperMins_Tonnes , Avg_FCR_LTRS_MINS , (Sum_FUELSUSEDINCYCLE / No_of_Trips ) AS Avg_Ltrs_per_trips , IF( SMU_payload is NULL , 0, 1 ) AS payload_dataDummy  FROM PayloadSummary1 ORDER BY Machine_Ser ,StartDate,EndDate, DateValid ")
****************************************************************************************************************************************/
		

		val  PayloadValidDatesTables = sqlContext.sql("SELECT  Machine_Ser , DateValid FROM payloadSummary_formaula ") 
		
		PayloadValidDatesTables.registerTempTable("PayloadValidDatesTables")

		val TTRERCJoinsValidDates = sqlContext.sql("SELECT Machine_Ser , DateValid , DownDate , TTR_inDays , TimeInOperation , TTR_Operation_Hours , Bin_TTR   FROM  PayloadValidDatesTables LEFT OUTER JOIN TTRERCTable ON  Machine_Ser = Machine_TTR  AND DateValid = DownDate  ")
		TTRERCJoinsValidDates.registerTempTable("TTRERCJoinsValidDates")
		
		val MTTR_BinnedTable = sqlContext.sql("SELECT Machine_Ser AS Machine_MTTR , Bin_TTR , avg( TTR_inDays ) AS Avg_TTR_inDays , avg( TimeInOperation ) AS avg_TimeInOperation , avg( TTR_Operation_Hours ) AS Avg_TTR_Operation_Hours  FROM TTRERCJoinsValidDates GROUP BY Machine_Ser ,Bin_TTR ")

		MTTR_BinnedTable.registerTempTable("MTTR_BinnedTable")


/**************************************************   MTBR ***************************************************************

	val MTBR_result = sqlContext.sql("SELECT serial , date , SMU , diff_Days , diff_SMU , getBin_MTBR( bin ) AS Bin_MTBR , TBR_Hour , TBR_InDays , count_ AS No_Of_Repairs  FROM table_1 JOIN table_2 ON serial = serial1 AND date = date1 ")

***********************************************************************************************************************/

 val MTBR_result_consized  = sqlContext.sql("SELECT serial , date , Bin_MTBR  ,TBR_Hour , TBR_InDays ,  No_Of_Repairs FROM MTBR_result ")

MTBR_result_consized.registerTempTable("MTBR_result_consized")

val MTBR_result_consized_Joined_PayloadValidDatesTables = sqlContext.sql("SELECT Machine_Ser , DateValid , Bin_MTBR  ,TBR_Hour , TBR_InDays ,  No_Of_Repairs  FROM PayloadValidDatesTables LEFT OUTER JOIN MTBR_result_consized ON Machine_Ser = serial AND DateValid = date ") 

MTBR_result_consized_Joined_PayloadValidDatesTables.registerTempTable("MTBR_result_consized_Joined_PayloadValidDatesTables")


val MTBR_BinnedTable = sqlContext.sql("SELECT Machine_Ser AS Machine_MTBR ,  Bin_MTBR , avg( TBR_InDays ) AS Avg_TBR_InDays , avg( TBR_Hour ) AS Avg_TBR_Hour , avg( No_Of_Repairs ) AS  Avg_No_Of_Repairs  FROM MTBR_result_consized_Joined_PayloadValidDatesTables  GROUP BY Machine_Ser , Bin_MTBR ")



MTBR_BinnedTable.registerTempTable("MTBR_BinnedTable")

val MTBR_MTTR_JOIN = sqlContext.sql("SELECT Machine_MTBR , Machine_MTTR ,  Bin_TTR , Bin_MTBR FROM MTTR_BinnedTable  JOIN MTBR_BinnedTable ON Machine_MTBR = Machine_MTTR AND  Bin_TTR = Bin_MTBR ORDER BY  Machine_MTBR , Bin_TTR ")

println ( "table1 =    "  + MTTR_BinnedTable.count + "  MTBR_BinnedTable =  " + MTBR_BinnedTable.count + " Joins = " +  MTBR_MTBR_JOIN.count )






