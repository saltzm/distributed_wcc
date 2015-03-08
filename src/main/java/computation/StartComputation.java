package computation;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.DegreeMessage;
import utils.ArrayPrimitiveWritable;
import utils.NeighborUtils;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

public class StartComputation 
  extends BasicComputation<IntWritable, WccVertexData, NullWritable,
          DegreeMessage> {

    //TODO: Put in superclass
    @Override
    public void postSuperstep() {
      double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
      double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
      aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
    }

  // Isolate any vertex with one or fewer edges and vote to halt
  @Override
  public void compute(Vertex<IntWritable, WccVertexData, NullWritable> vertex,
      Iterable<DegreeMessage> messages) {
    // No triangles possible. Automatically in ISOLATED_COMMUNITY
    if (vertex.getNumEdges() == 0) {
        vertex.voteToHalt();
    } else {
      sendMessageToAllEdges(vertex, new DegreeMessage(vertex.getId().get(), vertex.getNumEdges()));
      aggregate(WccMasterCompute.MAX_DEGREE, new LongWritable(vertex.getNumEdges()));
    }
  }
}
