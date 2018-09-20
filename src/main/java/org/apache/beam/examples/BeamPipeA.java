/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.windowing.Window;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import static junit.framework.Assert.assertNotNull;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.junit.Test;
import java.util.Date;
import org.joda.time.DateTime;


/**
 *
 * @author david
 */



public class BeamPipeA {
    
  private static final Logger LOG = LoggerFactory.getLogger(BeamPipeA.class);
 
    private static class GetMessage
    {
        private String order_number;
        private String service_type;
        private String driver_id;
        private String customer_id;
        private String service_area_name;
        private String payment_type;
        private String status;
        private String event_timestamp;
        
        
        public String getorder_number() {
            return order_number;
        }
        public void setorder_number(String order_number) {
            this.order_number = order_number;
        }
        
        public String getservice_type() {
            return service_type;
        }
        public void setservice_type(String service_type) {
            this.service_type = service_type;
        }    
        
        public String getdriver_id() {
            return driver_id;
        }
        public void setdriver_id(String driver_id) {
            this.driver_id = driver_id;
        }        

        public String getcustomer_id() {
            return customer_id;
        }
        public void setcustomer_id(String customer_id) {
            this.customer_id = customer_id;
        }

        public String getservice_area_name() {
            return service_area_name;
        }
        public void setservice_area_name(String service_area_name) {
            this.service_area_name = service_area_name;
        }

        public String getpayment_type() {
            return payment_type;
        }
        public void setpayment_type(String payment_type) {
            this.payment_type = payment_type;
        }

        public String getstatus() {
            return status;
        }
        public void setstatus(String status) {
            this.status = status;
        }

        public String getevent_timestamp() {
            return event_timestamp;
        }
        public void setevent_timestamp(String event_timestamp) {
            this.event_timestamp = event_timestamp;
        }

    }

    
     static class ParseJsonFn extends DoFn<String, String> {

        @ProcessElement
        public void processElement(@Element String in, OutputReceiver<String> out) throws IOException {
      
        ObjectMapper mapper = new ObjectMapper();
        
        // load json string to model
        GetMessage getMessage = mapper.readValue(in, GetMessage.class);
        
        // extract event timestamp
        DateTime dt = new DateTime(getMessage.getevent_timestamp());
        org.joda.time.Instant test = dt.toInstant();
        
        try {
        //add event timestamp to the pcollection of composite keys
        out.outputWithTimestamp(getMessage.getservice_area_name().toString() + "," + getMessage.getpayment_type().toString() + "," + getMessage.getstatus().toString(), test);
          
        } catch (java.lang.IllegalArgumentException exception) {

              LOG.info("Exception found:", exception);
        }
        
        }

   
    }
  
  
  public interface BeamPipeAOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    //@Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }


  public static void main(String[] args) {
    BeamPipeAOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BeamPipeAOptions.class);

    Pipeline p = Pipeline.create(options);

    // Continously watch for new files every n seconds
    PCollection<String> myInput = p.apply("ReadLines", TextIO.read().from(options.getInputFile())
        .watchForNewFiles(
        Duration.standardSeconds(5),
        Watch.Growth.<String>never())
    );
            
            

    PCollection<String> composite_keys = myInput.apply(ParDo.of(new ParseJsonFn()));  
    
    PCollection<KV<String, Long>> counted = composite_keys
            .apply(
            "Standard Window",
            Window.<String>into(FixedWindows.of(Duration.standardMinutes(5)))
                // Get periodic results every ten minutes.
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(1))))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.standardMinutes(10)))    
            .apply(Count.perElement());   
   
    
    PCollection<String> myInput3 = counted
    .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()));
    

    //fixedWindowedItems
    myInput3.apply("WriteCounts", TextIO.write().withWindowedWrites().withNumShards(1).to(options.getOutput()));

    
    p.run().waitUntilFinish();

  }
    
    
}
