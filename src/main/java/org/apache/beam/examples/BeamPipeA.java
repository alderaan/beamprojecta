/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.beam.examples;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.api.client.json.Json;
import com.google.api.services.bigquery.model.TableRow;
import java.util.Arrays;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.windowing.Window;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import static junit.framework.Assert.assertNotNull;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.junit.Test;


/**
 *
 * @author david
 */





public class test {
    
  private static final Logger LOG = LoggerFactory.getLogger(test.class);
   
    static class JsonToKV extends DoFn<String, KV<String, String>> {
      @ProcessElement
      public void processElement(@Element String word, OutputReceiver<KV<String, String>> out) {
        // Use OutputReceiver.output to emit the output element.
        
        String[] myString = word.replace(",","").replace("\"","").replaceAll("\\s+","").split(":");
        
        if(myString.length == 2)
        {
            out.output(KV.of(myString[0], myString[1]));
        }
           
      }
}
    
    
  
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
          
        GetMessage getMessage = mapper.readValue(in, GetMessage.class);
        out.output(getMessage.getservice_area_name().toString() + ":" + getMessage.getpayment_type().toString() + ":" + getMessage.getstatus().toString());
          
        }
    }
  
  
  public interface WordCountOptions extends PipelineOptions {

    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }


  public static void main(String[] args) {
    WordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

    
    LOG.info("Logging works ");
    
    Pipeline p = Pipeline.create(options);

    PCollection<String> myInput = p.apply("ReadLines", TextIO.read().from(options.getInputFile())
        .watchForNewFiles(
        // Check for new files every 30 seconds
        Duration.standardSeconds(1),
        // Never stop checking for new files
        Watch.Growth.<String>never())
    );
            
            
    


    PCollection<String> jsons = myInput.apply(ParDo.of(new ParseJsonFn()));  
    
    PCollection<KV<String, Long>> counted = jsons
            .apply(
            "LeaderboardUserGlobalWindow",
            //Window.<String>into(new GlobalWindows())
            Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                // Get periodic results every ten minutes.
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1))))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.standardMinutes(10)))    
            .apply(Count.perElement());   
   
    
    PCollection<String> myInput3 = counted
    .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()));
    

    PCollection<KV<String, String>> myInput2 = myInput.apply(
        ParDo
        .of(new JsonToKV()));

    /*PCollection<KV<String, Iterable<String>>> groupedWords = myInput2.apply(
    GroupByKey.create());*/
    
    
   
    
   
    /*PCollection<String> fixedWindowedItems = myInput3.apply(
        Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))));*/

    
    //fixedWindowedItems
    myInput3.apply("WriteCounts", TextIO.write().withWindowedWrites().withNumShards(3).to(options.getOutput()));
    //myInput3.apply("WriteCounts", TextIO.write().to(options.getOutput()));
    
    p.run().waitUntilFinish();

  }
    
    
}
