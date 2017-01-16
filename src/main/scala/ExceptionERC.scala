package catetl

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._

object ExceptionERC_class {
	
	
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


def evaluteExceptionERC ( sc : org.apache.spark.SparkContext  , sqlContext : org.apache.spark.sql.SQLContext   ) : org.apache.spark.sql.SchemaRDD  =  {


		import sqlContext._ 
		import sqlContext.createSchemaRDD

	
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

val masterFlatRDD = machineRDD.flatMapValues( x => x ) 

     println ( masterFlatRDD.count )


val masterMachineRDD = masterFlatRDD.map( x =>  MasterTable ( x._1 , x._2._2 , x._2._1 ) ) 



aggregatedException.registerTempTable("aggregatedExceptionTable")
masterMachineRDD.registerTempTable("masterMachineRDDTable")

val leftJoined = sqlContext.sql("SELECT machineNo ,startDate, dateValid ,Critical_Fluid ,Critical_Event , Critical_Trend , Critical_Datalogger , Critical_All , Fluid , Event , Trend , Datalogger , Date , Critical_Fluid2 , Critical_Event2 , Critical_Trend2 ,  Critical_Datalogger2 , Critical_All2     FROM masterMachineRDDTable LEFT JOIN aggregatedExceptionTable ON  masterMachineRDDTable.machineNo = aggregatedExceptionTable.Serial_Number  AND masterMachineRDDTable.dateValid = aggregatedExceptionTable.Date ")

leftJoined.registerTempTable("leftJoinedTable")

	leftJoined
	
	}


}
