
package computation.wcc_iteration;

import vertex.WccVertexData;
import messages.WccMessage;
import messages.TransferMessage;
import messages.CommunityInitializationMessage;
import messages.CommunityInitMessageArrayWritable;
import computation.WccMasterCompute;
import utils.ArrayPrimitiveWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.graph.Vertex;

public class StartWccIterationComputation extends AbstractComputation<
    IntWritable, WccVertexData, NullWritable, WccMessage, TransferMessage> {
    
    //TODO: Put in superclass
    @Override
    public void postSuperstep() {
      double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
      double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
      aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
    }

    @Override
    public void compute(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            Iterable<WccMessage> messages) {

        WccVertexData vData = vertex.getValue();
        vData.setCommInitNeighbors(new CommunityInitMessageArrayWritable( 
                    new CommunityInitializationMessage[0])); // Don't need it anymore 
        vData.saveCurrentCommunityAsBest();

        sendMessageToAllEdges(vertex,
                new TransferMessage(vertex.getId().get(), vData.getCommunity()));
    }
}
