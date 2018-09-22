/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dl_productions;
import org.dl_productions.BeamPipeA;
 import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
 /**
 *
 * @author david
 */
@RunWith(JUnit4.class)
public class BeamPipeATest {
    
@Rule public TestPipeline p = TestPipeline.create();
 // Input Json Strings to test
static final String[] FileInputString = new String[] {
    "{\"service_area_name\": \"JAKARTA\", \"payment_type\": \"GET_CASH\", \"status\": \"COMPLETED\", \"event_timestamp\": \"2018-09-29T14:00:00.000Z\"}",
};
 static final List<String> FileInputStringList = Arrays.asList(FileInputString);
 // Expected output from DoFn

 static final String[] CompositeKeyOutExp = new String[] {
    "JAKARTA,GET_CASH,COMPLETED"
};
 
 static final List<String> CompositeKeyOutExpList = Arrays.asList(CompositeKeyOutExp);
  
 @Test
public void ParseJsonFnTest() {
     PCollection<String> input = p.apply(Create.of(FileInputStringList)).setCoder(StringUtf8Coder.of());
    PCollection<String> output = input.apply(ParDo.of(new BeamPipeA.ParseJsonFn())); 
   
    PAssert.that(output)
    .containsInAnyOrder(
      CompositeKeyOutExpList
    );
  
  // Run the pipeline.
  p.run();
}
}