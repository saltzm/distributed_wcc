
package computation.preprocessing;    

import computation.WccMasterCompute;

import messages.WccMessage;
import messages.CommunityInitializationMessage;
import vertex.WccVertexData;
import messages.AdjacencyListMessage;
import utils.ArrayPrimitiveWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.utils.MemoryUtils;


public class FinishPreprocessingComputation extends AbstractComputation<IntWritable,
       WccVertexData, NullWritable, AdjacencyListMessage, WccMessage> {

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
            Iterable<AdjacencyListMessage> messages) {

        boolean vertexContinues = finishPreprocessing(vertex);

        WccVertexData vData = vertex.getValue();
        vData.setHigherDegreeNeighbors(new ArrayPrimitiveWritable(new int[0])); 
        vData.setNeighbors(new ArrayPrimitiveWritable(new int[0])); 
        // move on to community initialization
        if (vertexContinues) { 
            sendMessageToAllEdges(vertex, new WccMessage(
                new CommunityInitializationMessage(vertex)));
        }
    }
    
    private boolean finishPreprocessing(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex) {
        WccVertexData vData = vertex.getValue();
        // TODO comment out
        if (vData.getT() % 2 != 0) {
            System.out.println("Preprocessing error: t should be even for vertex " + 
                    vertex.getId() + " with t = " + vData.getT());
            System.exit(-1);
        }

        // each triangle is counted twice, one time for each other vertex in the
        // triangle. so to get the true t, must divide by two
        vData.setT(vData.getT()/2);
        vData.setVt(vertex.getNumEdges());

        // t = 0 implies that it is no longer connected to any other
        // vertices after preprocessing and so it will belong to its own
        // community
        if (vData.getT() == 0) {
            vertex.voteToHalt();
            return false;
        } else {
            int numNeighbors = vertex.getNumEdges();
            // TODO get rid of ?
            if (numNeighbors < 2) {
                System.out.println("Preprocessing error: numNeighbors for node " + 
                        vertex.getId() + " with t = " + vData.getT() + " and vt = " + 
                        vData.getVt() + " should be >= 2");
                System.exit(-1);
            }

            double clusteringCoefficient = (numNeighbors < 2) ?  0.0 :
                vData.getT() / (numNeighbors * (numNeighbors - 1) / 2.0);
             
            vData.setClusteringCoefficient(clusteringCoefficient);

            aggregate(WccMasterCompute.GRAPH_CLUSTERING_COEFFICIENT, 
                      new DoubleWritable(clusteringCoefficient));
            return true;
        }
    }
}
