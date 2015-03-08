
package computation.wcc_iteration;

import computation.WccMasterCompute;
import aggregators.CommunityAggregatorData;
import vertex.WccVertexData;
import messages.TransferMessage;
import utils.ArrayPrimitiveWritable;
import utils.NeighborUtils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.graph.Vertex;

import java.util.Map;

public class UpdateCommunityInfoComputation extends AbstractComputation<
    IntWritable, WccVertexData, NullWritable, TransferMessage, ArrayPrimitiveWritable> {

    private boolean foundNewBest;

    @Override
    public void preSuperstep() {
      foundNewBest = ((BooleanWritable) 
          getAggregatedValue(WccMasterCompute.FOUND_NEW_BEST_PARTITION)).get(); 
    }

    //TODO: Put in superclass
    @Override
    public void postSuperstep() {
      double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
      double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
      aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
    }

    /**
     * If the previous community yielded a new best WCC, save the previous
     * community as best. Based on incoming messages from neighbors with changed
     * communities, update the neighbor community map for the vertex. Publish
     * aggregates for its community.
     */
    @Override
    public void compute(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            Iterable<TransferMessage> messages) {

        WccVertexData vData = vertex.getValue();

        if (foundNewBest) {
            vData.savePreviousCommunityAsBest();
        }

        updateNeighborCommunityMap(vData, messages);
    }

    /**
     *  Given the data for a vertex and a group of TransferMessages from
     *  neighboring nodes, adjust the neighborCommunityMap accordingly
     */
    private void updateNeighborCommunityMap(WccVertexData vData,
            Iterable<TransferMessage> messages) {
        for (TransferMessage tm : messages) {
            vData.getNeighborCommunityMap().put(
                    new IntWritable(tm.getSourceId()),
                    new IntWritable(tm.getCommunity())); 
        }
    }
}
