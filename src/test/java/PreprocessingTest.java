/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//import vertex.WccVertexData;
//
//import org.apache.giraph.examples.*;
//import org.apache.giraph.conf.GiraphConfiguration;
//import org.apache.giraph.edge.ByteArrayEdges;
//import org.apache.giraph.edge.EdgeFactory;
//import org.apache.giraph.edge.EdgeNoValue;
//import org.apache.giraph.graph.DefaultVertex;
//import org.apache.giraph.graph.Vertex;
//import org.apache.giraph.io.formats.JsonLongDoubleFloatDoubleVertexInputFormat;
//import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
//import org.apache.giraph.utils.InternalVertexRunner;
//import org.apache.giraph.utils.MockUtils;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.json.JSONArray;
//import org.json.JSONException;
//import org.junit.Test;
//import org.mockito.Mockito;
//
//import com.google.common.collect.Iterables;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//
//import java.util.Map;
//import java.util.regex.Pattern;
//import java.util.regex.Matcher;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
//
//
///**
// * Must comment out everything after superstep 1 to work!
// */
//public class PreprocessingTest {
//
////  /**
////   * Test the behavior when a shorter path to a vertex has been found
////   */
////  @Test
////  public void testOnShorterPathFound() throws Exception {
////    Vertex<IntWritable, DoubleWritable, FloatWritable> vertex =
////        new DefaultVertex<IntWritable, DoubleWritable, FloatWritable>();
////    SimpleShortestPathsComputation computation =
////        new SimpleShortestPathsComputation();
////    MockUtils.MockedEnvironment<IntWritable, DoubleWritable, FloatWritable,
////        DoubleWritable> env = MockUtils.prepareVertexAndComputation(vertex,
////        new IntWritable(7L), new DoubleWritable(Double.MAX_VALUE), false,
////        computation, 1L);
////    Mockito.when(SOURCE_ID.get(env.getConfiguration())).thenReturn(2L);
////
////    vertex.addEdge(EdgeFactory.create(
////        new IntWritable(10L), new FloatWritable(2.5f)));
////    vertex.addEdge(EdgeFactory.create(
////        new IntWritable(20L), new FloatWritable(0.5f)));
////
////    computation.compute(vertex, Lists.newArrayList(new DoubleWritable(2),
////        new DoubleWritable(1.5)));
////
////    assertTrue(vertex.isHalted());
////    assertEquals(1.5d, vertex.getValue().get(), 0d);
////
////    env.verifyMessageSent(new IntWritable(10L), new DoubleWritable(4));
////    env.verifyMessageSent(new IntWritable(20L), new DoubleWritable(2));
////  }
////
//  /**
//   * Test the behavior when a new, but not shorter path to a vertex has been
//   * found.
//   */
////  @Test
////  public void testOnNoShorterPathFound() throws Exception {
////    Vertex<IntWritable, DoubleWritable, FloatWritable> vertex =
////        new DefaultVertex<IntWritable, DoubleWritable, FloatWritable>();
////    SimpleShortestPathsComputation computation =
////        new SimpleShortestPathsComputation();
////    MockUtils.MockedEnvironment<IntWritable, DoubleWritable, FloatWritable,
////        DoubleWritable> env = MockUtils.prepareVertexAndComputation(vertex,
////        new IntWritable(7L), new DoubleWritable(0.5), false, computation, 1L);
////    Mockito.when(SOURCE_ID.get(env.getConfiguration())).thenReturn(2L);
////
////    vertex.addEdge(EdgeFactory.create(new IntWritable(10L),
////        new FloatWritable(2.5f)));
////    vertex.addEdge(EdgeFactory.create(
////        new IntWritable(20L), new FloatWritable(0.5f)));
////
////    computation.compute(vertex, Lists.newArrayList(new DoubleWritable(2),
////        new DoubleWritable(1.5)));
////
////    assertTrue(vertex.isHalted());
////    assertEquals(0.5d, vertex.getValue().get(), 0d);
////
////    env.verifyNoMessageSent();
////  }
////
//
//    @Test
//    public void testToyDataAfterPreprocessing_oneNode() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf();        
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Long, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(0, 0, 0.0, 0), resMap.get(0L)); 
//    }
//
//
//    @Test
//    public void testToyDataAfterPreprocessing_twoNodes() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1",
//            "1 0"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf();        
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Long, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(0, 0, 0.0, 0), resMap.get(0L)); 
//        assertEquals(new WccVertexData(0, 0, 0.0, 1), resMap.get(1L)); 
//    }
//
//    @Test
//    public void testToyDataAfterPreprocessing_3Nodes0Triangles() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1",
//            "1 0 2",
//            "2 1"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf();        
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Long, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(0, 0, 0.0, 0), resMap.get(0L)); 
//        assertEquals(new WccVertexData(0, 0, 0.0, 1), resMap.get(1L)); 
//        assertEquals(new WccVertexData(0, 0, 0.0, 2), resMap.get(2L)); 
//    }
//
//    @Test
//    public void testToyDataAfterPreprocessing_3Nodes1Triangle() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2",
//            "1 0 2",
//            "2 0 1"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf();        
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Long, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(1, 2, 1.0, 0), resMap.get(0L)); 
//        assertEquals(new WccVertexData(1, 2, 1.0, 1), resMap.get(1L)); 
//        assertEquals(new WccVertexData(1, 2, 1.0, 2), resMap.get(2L)); 
//    }
//  /**
//   * A local integration test on toy data
//   */
//  @Test
//  public void testToyDataAfterPreprocessing() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2 3 5",
//            "1 0 2 3",
//            "2 0 1 4 5", 
//            "3 0 1",
//            "4 2",
//            "5 0 2"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf();        
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Long, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(3, 4, 0.5, 0), resMap.get(0L)); 
//        assertEquals(new WccVertexData(2, 3, 2.0/3.0, 1), resMap.get(1L)); 
//        assertEquals(new WccVertexData(2, 3, 2.0/3.0, 2), resMap.get(2L)); 
//        assertEquals(new WccVertexData(1, 2, 1.0, 3), resMap.get(3L)); 
//        assertEquals(new WccVertexData(0, 0, 0.0, 4), resMap.get(4L)); 
//        assertEquals(new WccVertexData(1, 2, 1.0, 5), resMap.get(5L)); 
//    }
//
//}
