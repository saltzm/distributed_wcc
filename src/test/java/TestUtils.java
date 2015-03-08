
import vertex.WccVertexData;
import computation.StartComputation;
import computation.WccMasterCompute;
import io.IntWccVertexDataNullTextInputFormat;
import utils.IntNullHashSetEdges;
//import io.IntNullReverseTextEdgeInputFormat;

import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.io.formats.IntNullReverseTextEdgeInputFormat;
import org.apache.giraph.io.formats.IntNullNullTextInputFormat;
import com.google.common.collect.Maps;
import com.google.common.collect.Iterables;

import org.apache.giraph.conf.GiraphConfiguration;

import org.apache.giraph.aggregators.TextAggregatorWriter;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TestUtils {
    public static GiraphConfiguration getConf() {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(StartComputation.class);
        conf.setOutEdgesClass(IntNullHashSetEdges.class);
        conf.setVertexInputFormatClass(
            IntWccVertexDataNullTextInputFormat.class);
        conf.setVertexOutputFormatClass(
            IdWithValueTextOutputFormat.class);
//        conf.set("giraph.partitionClass", "org.apache.giraph.partition.ByteArrayPartition");
        conf.set("wcc.numPreprocessingSteps","10");
        conf.set("wcc.maxRetries","2");
        conf.set("wcc.numWccComputationSteps","2");
        conf.set("giraph.useSuperstepCounters", "false");
        conf.setMasterComputeClass(WccMasterCompute.class);
        return conf;
    }

    public static GiraphConfiguration getEdgeFormatConf() {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(StartComputation.class);
//        conf.setVertexInputFormatClass(
//            IntWccVertexDataNullTextInputFormat.class);
        conf.setOutEdgesClass(IntNullHashSetEdges.class);
        conf.set("wcc.numPreprocessingSteps","10");
        conf.set("wcc.maxRetries","3");
        conf.set("wcc.numWccComputationSteps","5");
        conf.setEdgeInputFormatClass(IntNullReverseTextEdgeInputFormat.class);
        conf.setVertexOutputFormatClass(
            IdWithValueTextOutputFormat.class);
        conf.setMasterComputeClass(WccMasterCompute.class);
        conf.set("giraph.useSuperstepCounters", "false");
        conf.setAggregatorWriterClass(TextAggregatorWriter.class);
        return conf;
    }

    public static Map<Integer, WccVertexData> parseResults(Iterable<String> results) {
        Map<Integer, WccVertexData> resMap =
            Maps.newHashMapWithExpectedSize(Iterables.size(results));
        Pattern p = 
            Pattern.compile("(.*)\\s*[(]t = (.*), vt = (.*), clusteringCoefficient = (.*), community = (.*), bestCommunity = (.*)[)]");
        for (String r : results) {
            System.err.println(r);
            Matcher m = p.matcher(r);
            boolean b = m.matches();
            int id = Integer.parseInt(m.group(1).trim());
            int t = Integer.parseInt(m.group(2).trim());
            int vt = Integer.parseInt(m.group(3).trim());
            double cc = Double.parseDouble(m.group(4).trim());
            int community = Integer.parseInt(m.group(5).trim());
            int bestCommunity = Integer.parseInt(m.group(6).trim());
            WccVertexData vData = new WccVertexData();
            vData.setT(t);
            vData.setVt(vt);
            vData.setClusteringCoefficient(cc);
            vData.setCommunity(community);
            vData.setBestCommunity(bestCommunity);
            resMap.put(id, vData);
        }
        return resMap;
    }
}
