
package computation.preprocessing;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.DegreeMessage;
import messages.TriangleCountMessage;
import utils.ArrayPrimitiveWritable;
import utils.NeighborUtils;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Processes DegreeMessages from previous superstep and stores neighbors with a
 * higher degree than the current vertex in the vertex's WccVertexData
 */
public class StartPreprocessingComputation 
  extends AbstractComputation<IntWritable, WccVertexData, NullWritable,
          DegreeMessage, TriangleCountMessage> {

  //TODO: Put in superclass
  @Override
  public void postSuperstep() {
    double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
    double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
    aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
  }

  @Override
  public void compute(Vertex<IntWritable, WccVertexData, NullWritable> vertex,
    Iterable<DegreeMessage> messages) {
    // Save all incoming vertex ids with degree higher than vertex
    ArrayList<Integer> higherDegreeNeighbors = new ArrayList();
    long degree = vertex.getNumEdges(); 

    for (DegreeMessage m : messages) {
      if (m.getDegree() > degree || 
          (m.getDegree() == degree && m.getSourceId() > vertex.getId().get())) {
        // Break ties on id
        higherDegreeNeighbors.add(m.getSourceId());
      }
    }

    int[] hdnArr = new int[higherDegreeNeighbors.size()];
    int i = 0;
    for (Integer hdn : higherDegreeNeighbors) {
      hdnArr[i] = hdn.intValue();
      i++;
    }

    ArrayPrimitiveWritable hdnWritable = new ArrayPrimitiveWritable(hdnArr);
    WccVertexData vData = vertex.getValue();
    vData.setHigherDegreeNeighbors(hdnWritable);
    vData.setNeighbors(NeighborUtils.getNeighborsWritableArray(vertex));

    aggregate(WccMasterCompute.PREPROCESSING_VERTICES_SENT, new LongWritable(hdnArr.length * degree));
  }
}
