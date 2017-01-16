package catetl

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._

object FluidERC_object {
	
	
	
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
			case e : java.lang.Exception => None 
		}
	}


	
	def myDouble( x : Any ) : Double = {

			try{ x.toString.toDouble 
			}
			catch {
				case p : java.lang.NullPointerException => 0.0
				case e : java.lang.Exception  => 0.0 
			}
	}


	
	def getDate ( str : Any ) : Option[java.sql.Date] = {
	
			try{
				Some(java.sql.Date.valueOf( str.toString ))
			}
			catch { 
				case e: java.lang.Exception  => None 
			}
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
			case e : java.lang.Exception  => None 
		}
	}

	
	def evaluteFluidERC( sc : org.apache.spark.SparkContext  , sqlContext : org.apache.spark.sql.SQLContext   ) : org.apache.spark.sql.SchemaRDD  = {
		
	import sqlContext._ 
	import sqlContext.createSchemaRDD
		
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

    machineOrderedRDD
		
		}
	
	
}
