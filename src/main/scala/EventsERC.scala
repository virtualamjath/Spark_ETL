package catetl

import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._

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


def endOvrSpd(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Eng OvrSpd")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}



def engPreLubeOvr(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Engine Pre-lube Override")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}



def grdLvlShut(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("GND LVL SHTDN")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {	case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}


def brkRealted(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Brk Related")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}





def hiOilTemp(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Hi Strg Oil Tmp")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}





def hiFuelWtr(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("HiFul/Wtr SpLvl")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}




def idlStndRely(x:String, y: Any): Option[Double] = {
	try{
	      if (x.equals("Idle Stdn Rly")) Some(y.toString.toDouble ) else Some(0.0)
	}
	catch {
		case p : java.lang.NullPointerException => None
		case e : java.lang.Exception => None
	}
}


	def payloadOvr(x:String, y: Any): Option[Double] = {
		try{
			if (x.equals("Pyld Ovrld Abuse")) Some(y.toString.toDouble ) else Some(0.0)
		}
		catch {
			case p : java.lang.NullPointerException => None
			case e : java.lang.Exception => None
		}
	}

def convert ( x: Any  ) : Option[Double] = {

	try {
		Some( x.toString.toDouble )
	}
	catch {
		case e : java.lang.Exception => Some(0.0)
	}

}



def evaluteEventsERC ( sc : org.apache.spark.SparkContext  , sqlContext : org.apache.spark.sql.SQLContext   ) : org.apache.spark.sql.SchemaRDD  = {
	
	 import sqlContext._ 
	 import sqlContext.createSchemaRDD
	 
	 
	sqlContext.registerFunction("endOvrSpd", endOvrSpd(_ :String, _ : Any))
	sqlContext.registerFunction("engPreLubeOvr", engPreLubeOvr(_ :String, _ : Any))
	sqlContext.registerFunction("grdLvlShut", grdLvlShut(_ :String, _ : Any))
	sqlContext.registerFunction("brkRealted", brkRealted(_ :String, _ : Any))
	sqlContext.registerFunction("hiOilTemp", hiOilTemp(_ :String, _ : Any))
	sqlContext.registerFunction("hiFuelWtr", hiFuelWtr(_ :String, _ : Any))
	sqlContext.registerFunction("idlStndRely", idlStndRely(_ :String, _ : Any))
	sqlContext.registerFunction("payloadOvr", payloadOvr(_ :String, _ : Any))

	 
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

	val masterFlatRDD = machineRDD.flatMapValues( x => x ) 


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


	val eventsCal = sqlContext.sql("SELECT MACH_SER_NO, StartDate, EndDate, DateValid, Date, Max_SMU, endOvrSpd(Abuse_Event,CountofAbuseEvents) as endOvrSpd1, engPreLubeOvr(Abuse_Event,CountofAbuseEvents) as engPreLubeOvr1, grdLvlShut(Abuse_Event,CountofAbuseEvents) as grdLvlShut1, brkRealted(Abuse_Event,CountofAbuseEvents) as brkRealted1, hiOilTemp(Abuse_Event,CountofAbuseEvents) as hiOilTemp1, hiFuelWtr(Abuse_Event,CountofAbuseEvents) as hiFuelWtr1,idlStndRely(Abuse_Event,CountofAbuseEvents) as idlStndRely1,payloadOvr(Abuse_Event,CountofAbuseEvents) as payloadOvr1 FROM aggregatedEventsTable")

	eventsCal.registerTempTable("eventsCalTable")

	
	sqlContext.registerFunction("convert" , convert( _ : Any) )


	val finalEventsOutput = sqlContext.sql("SELECT MACH_SER_NO, StartDate, EndDate, DateValid, Date, Max_SMU, convert(endOvrSpd1), convert(engPreLubeOvr1), convert(grdLvlShut1), convert(brkRealted1), convert(hiOilTemp1), convert(hiFuelWtr1), convert(idlStndRely1),convert( payloadOvr1), convert(( endOvrSpd1 + engPreLubeOvr1 + grdLvlShut1 + brkRealted1 + hiOilTemp1 + hiFuelWtr1 + idlStndRely1 + payloadOvr1 )) as TotalAbuseEvents FROM eventsCalTable ORDER BY MACH_SER_NO, StartDate, EndDate, DateValid ") 

	finalEventsOutput
	
	
	}
		
}







