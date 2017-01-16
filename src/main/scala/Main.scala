package catetl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.sql._

object Main {
	
	/* It walks through the Main Work Flow */
	
	def main( args : Array[String] ) = {
		
		 val sparkConf = new SparkConf().setAppName("CatETL")
        		//creates spark Context
       		 val sc = new SparkContext(sparkConf)
		
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		import sqlContext._ 
		import sqlContext.createSchemaRDD	
		
		/* It is for Payload ERC calculations */
		val payloadSummary_formaula = PayloadERC.evalutePayloadERC( sc , sqlContext )
		
		
		payloadSummary_formaula.registerTempTable("payloadSummary_formaula")
         /* It is the work flow after payload ERC calculations */
		val payload_Result_Variables = PostPayloadERC.postPayloadEvalute( sc , sqlContext , payloadSummary_formaula  )

				
		payload_Result_Variables.registerTempTable("payload_Result_Variables")
		
                 /* It is for Exception ERC Calculations */
		val leftJoined = ExceptionERC_class.evaluteExceptionERC( sc , sqlContext )
		
		/* It is Main payload variables left joined With Exception ERC variables */
		val Payload_Exception_Variables = PostException.postExceptionEvalution( sc, sqlContext , leftJoined , payloadSummary_formaula , payload_Result_Variables )
		
		
		Payload_Exception_Variables.registerTempTable("Payload_Exception_Variables")
		/* Event ERC calculations */
		val finalEventsOutput = EventERC.evaluteEventsERC( sc , sqlContext )	
		
		finalEventsOutput.registerTempTable("finalEventsOutput")
		/* cal culations of fluid after ERC calculation for main Work flow */
		val payload_Exception_Events_variable = PostEvents.postEventsEvalute( sc, sqlContext , finalEventsOutput , Payload_Exception_Variables )

		payload_Exception_Events_variable.registerTempTable("payload_Exception_Events_variable")
		
		/* base fluid ERC calculations */
		val machineOrderedTable = FluidERC_object.evaluteFluidERC( sc ,sqlContext )
		/* fluid ERC part 1 calculations */
		val  aggregateformula = FluidERCPart1.evaluteFluidERCPart1( sc, sqlContext , machineOrderedTable  ) 
		/* TBFC calculations for Fluid */
		val  TBFCResult       = FluidERCPart2.evaluteFluidERCPart2( sc, sqlContext , machineOrderedTable )

		/* payload Exception variable joining with Fluid variables */
		val payload_Exception_Events_Fluid_variables_TBFC = FluidIntegration.fluidIntegration( sc, sqlContext , aggregateformula , TBFCResult , payload_Exception_Events_variable  )
		
		/* calculations of Cumulative ERC and integrating in main flow */
		val payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable = Cumulative_ERC.evaluteCumulativeERC( sc, sqlContext , payload_Exception_Events_Fluid_variables_TBFC )		
		/* Repair History MTBR calculation */
		val MTBR_result = MTBR_class.evaluteMTBR( sc , sqlContext )
		/* Repair History MTTR calculations */
		val TTRERC =  MTTR_ERC.evaluteMTTR( sc , sqlContext  )
		/* joining the Repair History variable */
		val payload_Exception_Events_Fluid_TBFC_Cumulative_RepairHistory_variable = RepairHistory_ERC.evaluteRepairHistory ( sc , sqlContext , MTBR_result , TTRERC , payloadSummary_formaula ,payload_Exception_Events_Fluid_variables_TBFC_Cumulative_Variable  )	
		/* calculating Histogram variables */
		 val  histogramERC_Result  = Histogram_ERC.evaluteHistogram_ERC ( sc , sqlContext , payload_Exception_Events_Fluid_TBFC_Cumulative_RepairHistory_variable )
 
		


		}

	
	}