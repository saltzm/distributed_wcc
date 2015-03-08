
package computation.preprocessing;    

import computation.WccMasterCompute;

import messages.AdjacencyListMessage;
import messages.TriangleCountMessage;
import vertex.WccVertexData;
import utils.ArrayPrimitiveWritable;
import utils.NeighborUtils;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableUtils;

import java.util.HashSet;
//import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Set;
import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.AbstractComputation;

public class SendAdjacencyListComputation extends AbstractComputation<IntWritable,
       WccVertexData, NullWritable, TriangleCountMessage, AdjacencyListMessage> {

    // TODO: Refactor this stuff into superclass
    private boolean finished;
    private int stepsToDo;
    private int currentStep;

    public void preSuperstep() {
        stepsToDo = ((IntWritable)
                getAggregatedValue(WccMasterCompute.NUMBER_OF_PREPROCESSING_STEPS)).get();
        currentStep = ((IntWritable)
                getAggregatedValue(WccMasterCompute.INTERPHASE_STEP)).get();
        finished = (currentStep == stepsToDo);
        //TODO: I don't like this...
        if (finished) {
            aggregate(WccMasterCompute.PHASE_OVERRIDE, new BooleanWritable(true));
            aggregate(WccMasterCompute.NEXT_PHASE, 
              new IntWritable(WccMasterCompute.FINISH_PREPROCESSING));
        }
    }

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
            Iterable<TriangleCountMessage> messages) {
        if (currentStep > 0) updateTriangleCounts(vertex, messages);

        if (!finished) { 
            sendAdjacencySetToHigherNeighbors(vertex, currentStep, stepsToDo);
        }
    }

    private void updateTriangleCounts(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            Iterable<TriangleCountMessage> messages) {
        WccVertexData vData = vertex.getValue();
        int t = vData.getT();
        for (TriangleCountMessage m : messages) t += m.getT();
        vData.setT(t);
    }

    private void sendAdjacencySetToHigherNeighbors(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            int currentStep, int stepsToDo) {

        WccVertexData vData = vertex.getValue();
        int[] hdns = (int[]) vData.getHigherDegreeNeighbors().get();
        AdjacencyListMessage adjList = new AdjacencyListMessage(
                vertex.getId().get(),
                vData.getNeighbors());

        if (hdns.length != 0) {
            ArrayList<IntWritable> targets = new ArrayList();
            IntWritable targTemp = new IntWritable();
            for (int i = currentStep; i < hdns.length; i += stepsToDo) {
                int targetId = hdns[i];
                targTemp.set(targetId);

                // necessary because targetId could have been removed
                if (vertex.getEdgeValue(targTemp) != null) { //TODO maybe
                    targets.add(new IntWritable(targetId));
                    //sendMessage(targTemp, adjList);
                }
            }

            sendMessageToMultipleEdges(targets.iterator(), adjList); 
        }
    }
}
