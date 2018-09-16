/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.beam.examples;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    PCollection<String> myInput = p.apply("ReadLines", TextIO.read().from(options.getInputFile()));

    PCollection<KV<String, String>> myInput2 = myInput.apply(
        ParDo
        .of(new JsonToKV()));

    
    PCollection<String> myInput3 = myInput2
    .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, String> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()));
            
   
    myInput3.apply("WriteCounts", TextIO.write().to(options.getOutput()));
    
    p.run().waitUntilFinish();

  }
    
    
}
