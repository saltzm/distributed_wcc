
import vertex.WccVertexData;
import computation.WccMasterCompute;

import org.junit.Test;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntNullReverseTextEdgeInputFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.DefaultVertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.io.File;
import java.io.PrintWriter;

import org.apache.giraph.utils.InternalVertexRunner;
import java.util.*;
import java.util.regex.*;

public class CommunityInitializationTest {
//
//    @Test
//    public void testGetSortedCommunityInitializationMessages_init_returnsSorted(){
//        ArrayList<WCCMessage> messages = new ArrayList();
//        messages.add(new WCCMessage(new CommunityInitializationMessage(0L, 0, 0.0, false)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(1L, 0, 0.0, true)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(2L, 0, 0.1, false)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(3L, 0, 0.1, true)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(4L, 1, 0.1, false)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(5L, 1, 0.1, true)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(6L, 1, 0.0, false)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(7L, 1, 0.0, true)));
//
//        SortedSet<CommunityInitializationMessage> s = 
//            WccComputation.getSortedCommunityInitializationMessages(messages);
//        ArrayList<Integer> resultIds = new ArrayList();
//
//        for (CommunityInitializationMessage m : s) {
//            resultIds.add(m.getId());
//        }
//
//        Object[] expectedResult = new Object[] {0L, 6L, 2L, 4L, 1L, 7L, 3L, 5L};
//        assertArrayEquals(expectedResult, resultIds.toArray());
//    }
//    
//    @Test
//    public void testGetSortedCommunityInitializationMessages_modifyHead_returnsSorted(){
//        ArrayList<WCCMessage> messages = new ArrayList();
//        messages.add(new WCCMessage(new CommunityInitializationMessage(0L, 0, 0.0, false)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(1L, 0, 0.0, true)));
//        messages.add(new WCCMessage(new CommunityInitializationMessage(2L, 0, 0.1, true)));
//        TreeSet<CommunityInitializationMessage> s = 
//            WccComputation.getSortedCommunityInitializationMessages(messages);
//        CommunityInitializationMessage l = s.pollLast();
//        assertEquals("Last item's id should be 2", l.getId(), 2L);
//        l.setCenter(false);
//        s.add(l);
//        assertEquals("When highest element is updated, it should change " + 
//                "positions in the set", 1L, s.last().getId());
//    }
//
//    @Test
//    public void testChangingVertexDataIsReflectedInVertexWithoutSetVertex() {
//        Vertex<IntWritable, WccVertexData, NullWritable> vertex = new DefaultVertex();
//        WccVertexData d = new WccVertexData(1, 2, 1.0, 0L);
//        vertex.setValue(d);
//        d.setCommunity(1L); 
//        int expectedCommunity = 1L;
//        int result = vertex.getValue().getCommunity();
//        assertEquals("When a setter method is used on WccVertexData, the change " + 
//                "should be reflected in the vertex value without using " + 
//                "setVertex()", expectedCommunity, result); 
//    }
//
//    @Test
//    public void testWritableInstanceOfChangedMessage() {
//        Writable m = new ChangedMessage(0L);
//        assert(m instanceof ChangedMessage);
//    }
//
/////*************************************************************************************
//// *                                     TOY DATA
//// ************************************************************************************/

//    @Test
//    public void testToyDataAfterCommunityInitialization_oneNode() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf();        
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(0, 0, 0.0, 0), resMap.get(0L)); 
//    }
//
//
//
//    @Test
//    public void testToyDataAfterCommunityInitialization_twoNodes() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1",
//            "1 0"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf();        
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(0, 0, 0.0, 0), resMap.get(0L)); 
//        assertEquals(new WccVertexData(0, 0, 0.0, 1), resMap.get(1L)); 
//    }
//
//    @Test
//    public void testToyDataAfterCommunityInitialization_3Nodes0Triangles() throws Exception {
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
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(0, 0, 0.0, 0), resMap.get(0L)); 
//        assertEquals(new WccVertexData(0, 0, 0.0, 1), resMap.get(1L)); 
//        assertEquals(new WccVertexData(0, 0, 0.0, 2), resMap.get(2L)); 
//    }
//
//    @Test
//    public void testToyDataAfterCommunityInitialization_3Nodes1Triangle() throws Exception {
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
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assert(resMap.get(0L).getCommunity() == resMap.get(1L).getCommunity() &&
//               resMap.get(0L).getCommunity() == resMap.get(2L).getCommunity());
//    }

//  @Test
//  public void testToyDataAfterCommunityInitialization_6NodesComplex() throws Exception {
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
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        assertEquals(new WccVertexData(3, 4, 0.5, 5), resMap.get(0L)); 
//        assertEquals(new WccVertexData(2, 3, 2.0/3.0, 3), resMap.get(1L)); 
//        assertEquals(new WccVertexData(2, 3, 2.0/3.0, 5), resMap.get(2L)); 
//        assertEquals(new WccVertexData(1, 2, 1.0, 3), resMap.get(3L)); 
//        assertEquals(new WccVertexData(0, 0, 0.0, 4), resMap.get(4L)); 
//        assertEquals(new WccVertexData(1, 2, 1.0, 5), resMap.get(5L)); 
//    }

//  @Test
//  public void testToyDataAfterCommunityInitialization_12NodesComplex() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2",
//            "1 0 2",
//            "2 0 1 3 4 5", 
//            "3 2 5",
//            "4 2 5",
//            "5 2 3 4 6 7 8 9",
//            "6 5 7 10",
//            "7 5 6",
//            "8 5 9",
//            "9 5 8 11",
//            "10 6",
//            "11 9"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(1L, resMap.get(0L).getCommunity());
//        assertEquals(1L, resMap.get(1L).getCommunity());
//        assertEquals(4L, resMap.get(2L).getCommunity());
//        assertEquals(3L, resMap.get(3L).getCommunity());
//        assertEquals(4L, resMap.get(4L).getCommunity());
//        assertEquals(9L, resMap.get(5L).getCommunity());
//        assertEquals(7L, resMap.get(6L).getCommunity());
//        assertEquals(7L, resMap.get(7L).getCommunity());
//        assertEquals(9L, resMap.get(8L).getCommunity());
//        assertEquals(9L, resMap.get(9L).getCommunity());
//        assertEquals(10L, resMap.get(10L).getCommunity());
//        assertEquals(11L, resMap.get(11L).getCommunity());
//  }
//
//// Diamond with a center. Communities must propogate
//  @Test
//  public void testToyDataAfterCommunityInitialization_9NodesComplex() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 7 8",
//            "1 0 2 8",
//            "2 1 3 8",
//            "3 2 4 8",
//            "4 3 5 8",
//            "5 4 6 8",
//            "6 5 7 8",
//            "7 0 6 8",
//            "8 0 1 2 3 4 5 6 7"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(7L, resMap.get(0L).getCommunity());
//        assertEquals(1L, resMap.get(1L).getCommunity());
//        assertEquals(3L, resMap.get(2L).getCommunity());
//        assertEquals(3L, resMap.get(3L).getCommunity());
//        assertEquals(5L, resMap.get(4L).getCommunity());
//        assertEquals(5L, resMap.get(5L).getCommunity());
//        assertEquals(7L, resMap.get(6L).getCommunity());
//        assertEquals(7L, resMap.get(7L).getCommunity());
//        assertEquals(7L, resMap.get(8L).getCommunity());
//  }
//
//  @Test
//  public void testToyDataAfterCommunityInitialization_7SoloInCenter() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2",
//            "1 0 2 3",
//            "2 0 1 3",
//            "3 1 2 4 5",
//            "4 3 5 6",
//            "5 3 4 6",
//            "6 4 5"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(0L, resMap.get(0L).getCommunity());
//        assertEquals(0L, resMap.get(1L).getCommunity());
//        assertEquals(0L, resMap.get(2L).getCommunity());
//        assertEquals(3L, resMap.get(3L).getCommunity());
//        assertEquals(6L, resMap.get(4L).getCommunity());
//        assertEquals(6L, resMap.get(5L).getCommunity());
//        assertEquals(6L, resMap.get(6L).getCommunity());
//  }
//
//  @Test
//  public void testToyDataAfterCommunityInitialization_5SoloOnOutside() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2",
//            "1 0 2 3 4",
//            "2 0 1 3 4",
//            "3 1 2 4",
//            "4 1 2 3"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(0L, resMap.get(0L).getCommunity());
//        assertEquals(4L, resMap.get(1L).getCommunity());
//        assertEquals(4L, resMap.get(2L).getCommunity());
//        assertEquals(4L, resMap.get(3L).getCommunity());
//        assertEquals(4L, resMap.get(4L).getCommunity());
//  }
//
//  @Test
//  public void testToyDataAfterCommunityInitialization_9SoloInCenter() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2",
//            "1 0 2 3",
//            "2 0 1 3",
//            "3 1 2 4 5",
//            "4 3 5 6 7 8",
//            "5 3 4 6",
//            "6 4 5",
//            "7 4 8",
//            "8 4 7"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(0L, resMap.get(0L).getCommunity());
//        assertEquals(0L, resMap.get(1L).getCommunity());
//        assertEquals(0L, resMap.get(2L).getCommunity());
//        assertEquals(3L, resMap.get(3L).getCommunity());
//        assertEquals(8L, resMap.get(4L).getCommunity());
//        assertEquals(6L, resMap.get(5L).getCommunity());
//        assertEquals(6L, resMap.get(6L).getCommunity());
//        assertEquals(8L, resMap.get(7L).getCommunity());
//        assertEquals(8L, resMap.get(8L).getCommunity());
//  }
//
//
//  @Test
//  public void testToyDataAfterCommunityInitialization_3Triangles() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2",
//            "1 0 2",
//            "2 0 1 3 4",
//            "3 2 4",
//            "4 2 3 5 6",
//            "5 4 6",
//            "6 4 5"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(1L, resMap.get(0L).getCommunity());
//        assertEquals(1L, resMap.get(1L).getCommunity());
//        assertEquals(3L, resMap.get(2L).getCommunity());
//        assertEquals(3L, resMap.get(3L).getCommunity());
//        assertEquals(6L, resMap.get(4L).getCommunity());
//        assertEquals(6L, resMap.get(5L).getCommunity());
//        assertEquals(6L, resMap.get(6L).getCommunity());
//  }
//
//  @Test
//  public void testToyDataAfterCommunityInitialization_6NodeBridge() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2",
//            "1 0 2",
//            "2 0 1 3",
//            "3 2 4 5",
//            "4 3 5",
//            "5 3 4"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(2L, resMap.get(0L).getCommunity());
//        assertEquals(2L, resMap.get(1L).getCommunity());
//        assertEquals(2L, resMap.get(2L).getCommunity());
//        assertEquals(5L, resMap.get(3L).getCommunity());
//        assertEquals(5L, resMap.get(4L).getCommunity());
//        assertEquals(5L, resMap.get(5L).getCommunity());
//  }
//
//  @Test
//  public void testToyDataAfterCommunityInitialization_Pyramid() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2",
//            "1 0 4",
//            "2 0 3 4",
//            "3 2 4",
//            "4 1 2 3"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(0L, resMap.get(0L).getCommunity());
//        assertEquals(1L, resMap.get(1L).getCommunity());
//        assertEquals(4L, resMap.get(2L).getCommunity());
//        assertEquals(4L, resMap.get(3L).getCommunity());
//        assertEquals(4L, resMap.get(4L).getCommunity());
//  }
//  @Test
//  public void testToyDataAfterCommunityInitialization_DegreeTieBreak() throws Exception {
//        // a small four vertex graph
//        String[] graph = new String[] {
//            "0 1 2 3 4 5 6",
////            "0 1",
////            "0 2",
////            "0 3",
////            "0 4",
////            "0 5",
////            "0 6",
//            "1 0 6",
//            "2 0 3 4 5",
//            "3 0 2 4 5",
//            "4 0 2 3 5",
//            "5 0 2 3 4",
//            "6 0 1"
//        };
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//        // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf, graph);
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//
//        // verify results
//        
//        assertEquals(5L, resMap.get(0L).getCommunity());
//        assertEquals(6L, resMap.get(1L).getCommunity());
//        assertEquals(5L, resMap.get(2L).getCommunity());
//        assertEquals(5L, resMap.get(3L).getCommunity());
//        assertEquals(5L, resMap.get(4L).getCommunity());
//        assertEquals(5L, resMap.get(5L).getCommunity());
//        assertEquals(6L, resMap.get(6L).getCommunity());
//  }

//*************************************************************************************
// *                                     REAL DATA
// ************************************************************************************/

//    @Test
//    public void testFoodwebSCDEdgeFormat() throws Exception {
//        String inputDir = "/src/test/test_graphs/real_graphs/giraph_format/foodweb_scd_double_edge.txt";
//        Path filePath = new File(System.getProperty("user.dir") + inputDir).toPath();
//        Charset charset = Charset.defaultCharset();        
//        List<String> stringList = Files.readAllLines(filePath, charset);
//        String[] graph = stringList.toArray(new String[]{});
//
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//
//      //  conf.setEdgeInputFormatClass(
//      //      IntNullReverseTextEdgeInputFormat.class);
//         // run internally
//        Iterable<String> results = InternalVertexRunner.run(conf,
//                graph);
//
//        Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);
//    }
//    @Test
//    public void testAmazonGraphAfterCommunityInitializationNoRelabel() throws Exception {
//        String inputDir = "/../test_graphs/real_graphs/giraph_format/amazon_giraph_relabeled.txt";
//        Path filePath = new File(System.getProperty("user.dir") + inputDir).toPath();
//        Charset charset = Charset.defaultCharset();        
//        List<String> stringList = Files.readAllLines(filePath, charset);
//        String[] graph = stringList.toArray(new String[0]);
//        stringList.clear();
//        stringList = null;
//
//        System.gc();
//
//        GiraphConfiguration conf = TestUtils.getConf(); 
//        InternalVertexRunner.run(conf, graph);
//    }

    @Test
    public void testFoodwebGraphAfterCommunityInitialization() throws Exception {
        String inputDir       = "/../../branches/test_graphs/real_graphs/scd_format/foodweb.txt";
        String statsOutputDir = "/../../branches/test_graphs/real_graphs/output/gir_out/foodweb_giraph_stats.txt";
        String commsOutputDir = "/../../branches/test_graphs/real_graphs/output/gir_out/foodweb_giraph_comms.txt";
        testRealGraphAfterCommunityInitialization(inputDir, statsOutputDir, commsOutputDir);
    }

//    @Test
//    public void testFoodwebGraphAfterCommunityInitializationEdgeInputFormat() throws Exception {
//        String inputDir = "/../test_graphs/real_graphs/scd_format/foodweb.txt";
//        String statsOutputDir = "/../test_graphs/real_graphs/output/gir_out/foodweb_giraph_stats.txt";
//        String commsOutputDir = "/../test_graphs/real_graphs/output/gir_out/foodweb_giraph_comms.txt";
//
//        Path filePath = new File(System.getProperty("user.dir") + inputDir).toPath();
//        Charset charset = Charset.defaultCharset();        
//        List<String> stringList = Files.readAllLines(filePath, charset);
//        String[] edges = stringList.toArray(new String[0]);
//
//        GiraphConfiguration conf = TestUtils.getEdgeFormatConf();
//        InternalVertexRunner.run(conf, new String[]{}, edges);
//    }

//    @Test
//    public void testAmazonGraphAfterCommunityInitialization() throws Exception {
//        String inputDir = "/../test_graphs/real_graphs/scd_format/amazon.txt";
//        String statsOutputDir = "/../test_graphs/real_graphs/output/gir_out/amazon_giraph_stats.txt";
//        String commsOutputDir = "/../test_graphs/real_graphs/output/gir_out/amazon_giraph_comms.txt";
//        testRealGraphAfterCommunityInitialization(inputDir, statsOutputDir, commsOutputDir);
//    }

//    @Test
//    public void testLiveJournalGraphAfterCommunityInitialization() throws Exception {
//        String inputDir = "/src/test/test_graphs/real_graphs/scd_format/livejournal.txt";
//        String statsOutputDir = "/src/test/test_graphs/real_graphs/output/gir_out/livejournal_giraph_stats.txt";
//        String commsOutputDir = "/src/test/test_graphs/real_graphs/output/gir_out/livejournal_giraph_comms.txt";
//        testRealGraphAfterCommunityInitialization(inputDir, statsOutputDir, commsOutputDir);
//    }

//    @Test
//    public void testDBLPGraphAfterCommunityInitialization() throws Exception {
//        String inputDir = "/src/test/test_graphs/real_graphs/scd_format/dblp.txt";
//        String statsOutputDir = "/src/test/test_graphs/real_graphs/output/gir_out/dblp_giraph_stats.txt";
//        String commsOutputDir = "/src/test/test_graphs/real_graphs/output/gir_out/dblp_giraph_comms.txt";
//        testRealGraphAfterCommunityInitialization(inputDir, statsOutputDir, commsOutputDir);
//    }

    private void testRealGraphAfterCommunityInitialization(
            String inputDir, String statsOutputDir, String commsOutputDir) 
    throws Exception {
        Path filePath = new File(System.getProperty("user.dir") + inputDir).toPath();
        Charset charset = Charset.defaultCharset();        
        List<String> stringList = Files.readAllLines(filePath, charset);
        String[] graph = stringList.toArray(new String[]{});

        // Relabel like SCD does
        HashMap<String, Integer> oldToNewLabelMap = new HashMap();

        String[] relabeledGraph = new String[graph.length];

        int i = 0;
        int maxId = 0;
        for (String s: graph) {
            String[] splitLine = s.split("\\s+");
            String source = splitLine[0].trim();
            String target = splitLine[1].trim();
            if(!oldToNewLabelMap.containsKey(source)) {
                oldToNewLabelMap.put(source, maxId);
                maxId++;
            } 
            if(!oldToNewLabelMap.containsKey(target)) {
                oldToNewLabelMap.put(target, maxId);
                maxId++;
            } 
            int relSourceId = oldToNewLabelMap.get(source);
            int relTargetId = oldToNewLabelMap.get(target);
            relabeledGraph[i] = relSourceId + " " + relTargetId;
            i++;
        }
        
        // Convert from SCD format into Giraph format
        String[] girFormatGraph =
            convertSCDInputFormatToGiraphInputFormat(relabeledGraph);

        GiraphConfiguration conf = TestUtils.getConf(); 

        // run internally
        Iterable<String> results = InternalVertexRunner.run(conf,
                girFormatGraph);

       Map<Integer, WccVertexData> resMap = TestUtils.parseResults(results);

        // Rerelabel map
       Map<Integer, String> newToOldLabelMap = new HashMap();
       for(Map.Entry<String, Integer> entry : oldToNewLabelMap.entrySet()){
           newToOldLabelMap.put(entry.getValue(), entry.getKey());
       }

       // Rerelabel and convert to scd style output
       // community -> set of vertex ids in community
       HashMap<Integer, HashSet<Integer>> communityMap = new HashMap();
       Set<Integer> soloComms = new HashSet();
       Set<Integer> commIds = new HashSet();

       for (Map.Entry<Integer, WccVertexData> entry : resMap.entrySet()) {
          Integer vid = entry.getKey();
          Integer originalVertexId = Integer.parseInt(newToOldLabelMap.get(vid));

          Integer commId = entry.getValue().getBestCommunity();
          commIds.add(commId);
          if (commId.intValue() == WccMasterCompute.ISOLATED_COMMUNITY) {
            System.out.println(vid + " is solo");
              soloComms.add(originalVertexId);
          } else {
//              Integer originalCommId = Integer.parseInt(newToOldLabelMap.get(commId));

              //if (!communityMap.containsKey(originalCommId)) {
              if (!communityMap.containsKey(commId)) {
                 // communityMap.put(originalCommId, new HashSet());
                  communityMap.put(commId, new HashSet());
              }
              // add vertex id to that community
              communityMap.get(commId).add(originalVertexId);
          }
       }

        PrintWriter out = new PrintWriter(System.getProperty("user.dir") + commsOutputDir);
        System.out.println("Unique comms: " + commIds.size());
        System.out.println("Non solo comms: " + communityMap.size());
        System.out.println("Solo comms: " + soloComms.size());
        for (HashSet<Integer> community : communityMap.values()) {
            out.println(community.toString().replaceAll("[,\\[\\]]", ""));
        }
        for (Integer soloCom : soloComms) {
            out.println(soloCom);
        }
        out.close();

        PrintWriter statsOut = new PrintWriter(System.getProperty("user.dir") + statsOutputDir);
        for (Map.Entry<Integer, WccVertexData> entry : resMap.entrySet()) {
            WccVertexData relData = entry.getValue();
            //Integer comm = relData.getCommunity();
            //if (comm != -1) {
            //    Integer originalCommId = Integer.parseInt(newToOldLabelMap.get());
            //    relData.setCommunity(originalCommId);
            //}
            statsOut.println(entry.getKey() + " -> " + newToOldLabelMap.get(entry.getKey()) + "\t" + entry.getValue());
        }

        statsOut.close();
  }

    private String[] convertSCDInputFormatToGiraphInputFormat(String[] graph) {
        HashMap<String, HashSet<String>> adjList = new HashMap();
        for (String s: graph) {
            String[] splitLine = s.split(" ");
            String source = splitLine[0].trim();
            String target = splitLine[1].trim();
            
            if (!adjList.containsKey(source)) adjList.put(source, new HashSet());
            if (!adjList.containsKey(target)) adjList.put(target, new HashSet());
            adjList.get(source).add(target);
            adjList.get(target).add(source);
        }

        String[] girFormatGraph = new String[adjList.values().size()]; 
        int count = 0;
        for (String source : adjList.keySet()) {
            girFormatGraph[count] = source + " " + adjList.get(source).toString().replaceAll("[,\\[\\]]", "");
            count++;
        }
        return girFormatGraph;
    }



}

