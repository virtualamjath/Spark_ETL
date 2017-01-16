package catetl

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst
import org.apache.spark.rdd._
import java.sql.Date
import org.apache.spark.sql._
import org.apache.spark.rdd._


object RepairHistory {

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

   def max_SMU( a : Double , b : Double , c : Double , d : Double , e : Double   ) : Double = { 
	List( a , b , c , d , e  ).max
   }

   def evaluteRepairHistory (sc : SparkContext , args : Array[String]) : org.apache.spark.rdd.RDD[String]  = {
		

		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext._ 		
		
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

		

		val cumulativeData = sc.textFile("/home/amjath/Desktop/RepairHistory/RHDATA/cumulative.csv" )
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



	sqlContext.registerFunction( "max_SMU" , max_SMU(  _ : Double , _ : Double , _ : Double , _ : Double , _ : Double  ) )

	val maxSMURDD = sqlContext.sql( "SELECT  Machine_Repair , date_repair ,  SMURepair , max_SMU ( SMURepair , SMUCumulative , SMUEvents , SMUPayload , SMUFluid , SMUHist ) AS MAXSMU FROM RCEPFJoinsH ORDER BY Machine_Repair , date_repair, MAXSMU " )


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
  	
	val result = sqlContext.sql("SELECT serial , date , SMU , diff_Days , diff_SMU , bin , TBR_Hour , TBR_InDays , count_  FROM table_1 JOIN table_2 ON serial = serial1 AND date = date1   ORDER BY serial , date ")

	 result.registerTempTable( "resultTable")
	 
	sqlContext.sql("SELECT serial , date , SMU , TBR_Hour , TBR_InDays , bin , count_  As No_Repairs FROM  resultTable " ).map( x => x.mkString(",") ) 

    }

	
	def main( args : Array[String] ) {
		
		Logger.getLogger("org").setLevel(Level.ERROR)
        	Logger.getLogger("akka").setLevel(Level.ERROR)
    
        	//set up sparkconf
        	val sparkConf = new SparkConf().setAppName("CatETL")
        	//creates spark Context
       		val sc = new SparkContext(sparkConf)
		
		evaluteRepairHistory(sc , args ).coalesce(1).saveAsTextFile( "/home/amjath/Re/some" ) 
	}
	

}

