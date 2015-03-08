
package computation.wcc_iteration;

import static computation.WccMasterCompute.ISOLATED_COMMUNITY;

import computation.WccMasterCompute;
import vertex.WccVertexData;
import messages.TransferMessage;
import aggregators.CommunityAggregatorData;
import utils.ArrayPrimitiveWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeSet;

public class ChooseNextCommunityComputation extends AbstractComputation<
    IntWritable, WccVertexData, NullWritable, ArrayPrimitiveWritable, TransferMessage> {

    private MapWritable commAggMap; 
    private double globalCC;

    public void preSuperstep() {
      commAggMap = getAggregatedValue(WccMasterCompute.COMMUNITY_AGGREGATES); 
      globalCC = ((DoubleWritable) getAggregatedValue(WccMasterCompute.GRAPH_CLUSTERING_COEFFICIENT)).get() / 
                                   getTotalNumVertices();
    }

    //TODO: Put in superclass
    @Override
    public void postSuperstep() {
      double freeMemory = MemoryUtils.freeMemoryMB()/1000; // Mem in gigs
      double freeNotInHeap = (MemoryUtils.maxMemoryMB() - MemoryUtils.totalMemoryMB())/1000;
      aggregate(WccMasterCompute.MIN_MEMORY_AVAILABLE, new DoubleWritable(freeMemory + freeNotInHeap));
    }

    /**
     * Compute new best community and notify neighbors if it is different
     * from the current community
     */
    @Override
    public void compute(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            Iterable<ArrayPrimitiveWritable> messages) {

        computeAndSendLocalWcc(vertex);

        WccVertexData vData = vertex.getValue();
        int oldCommunity = vData.getCommunity();
        int newCommunity = bestMovement(vertex); 

        vData.updateCommunity(newCommunity); 

        if (oldCommunity != newCommunity) {
          sendMessageToAllEdges(vertex, 
            new TransferMessage(vertex.getId().get(), newCommunity));
        }                 
    }


    /**
     * Computes the local wcc value and sends it to an aggregator
     */
    private void computeAndSendLocalWcc(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex) {
      if (vertex.getValue().getCommunity() != ISOLATED_COMMUNITY) {
          double localWCC = computeWcc(vertex);
          aggregate(WccMasterCompute.WCC, new DoubleWritable(localWCC));
      }
    }

    /**
     *  Computes a vertex's wcc with respect to its current community, using the
     *  adjacency lists of its neighbors within the community to count triangles
     */
    public double computeWcc(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex) {

        WccVertexData vData = vertex.getValue();
        int globalT = vData.getT();
        int globalVt = vData.getVt();
        int communityT = vData.getCommunityT();
        int communityVt = vData.getCommunityVt();
        IntWritable community = new IntWritable(vData.getCommunity());
        CommunityAggregatorData commAggData = 
            (CommunityAggregatorData) commAggMap.get(community);
        int communitySize = commAggData.getSize(); 
        double rightDenom = (globalVt - communityVt + communitySize - 1);
        double wcc = (globalT == 0.0 || rightDenom == 0.0) ? 0.0 :  
            ((double) communityT/globalT) * ((double) globalVt / rightDenom);
        return wcc;
    }

    private int bestMovement(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex) {

        WccVertexData vData = (WccVertexData) vertex.getValue();
        int sourceC = vData.getCommunity();
        int bestC = vData.getCommunity();
        double wcc_r = computeWccRemovalIncrement(vertex);
        double wcc_t = 0;

        for (IntWritable c : getCandidateCommunities(vertex)) {
            double candidateWCCT = wcc_r + computeApproximateWCC_I(vertex, c);
            if (candidateWCCT > wcc_t) {
                wcc_t = candidateWCCT;
                bestC = c.get();
            }
        }

        // Transfer to best community
        if (wcc_t - wcc_r > 0.00001 && wcc_t > 0.0)  return bestC; 
        else if (wcc_r > 0.0) return ISOLATED_COMMUNITY;  // Remove
        else return sourceC;  // Stay 
    }

     /**
     * Calculates the approximation of the incremental improvement to global wcc
     * that would be incurred if this vertex were removed from its current
     * community and isolated.
     */
    private double computeWccRemovalIncrement(Vertex<IntWritable, WccVertexData, NullWritable> vertex) {
        IntWritable currentCommunity = new IntWritable(vertex.getValue().getCommunity());

        if (currentCommunity.get() == ISOLATED_COMMUNITY) return 0.0;

        CommunityAggregatorData curCommData = 
            (CommunityAggregatorData) commAggMap.get(currentCommunity);

        // Vertex statistics 
        int d_in = getNumberOfEdgesToCommunity(vertex, currentCommunity);
        int d_out = vertex.getNumEdges() - d_in;

        // Modify community to have statistics as if vertex were removed from it
        int sizeAfterRemoval = curCommData.getSize() - 1;
        int internalEdgesAfterRemoval = 2 * (curCommData.getNumberOfInternalEdgesUndirected() - d_in);
        int borderEdgesAfterRemoval = curCommData.getNumberOfBorderEdges() + d_in - d_out;

        CommunityAggregatorData commDataWithVertexRemoved = 
            new CommunityAggregatorData(sizeAfterRemoval, internalEdgesAfterRemoval, borderEdgesAfterRemoval);

        return -computeApproximateWCC_I(d_in, d_out, commDataWithVertexRemoved);
    }

    /**
     * Calculates the approximation of the incremental improvement to global wcc
     * that would be incurred if this vertex moved to the destination community
     * with id destCommunity.
     */
    private double computeApproximateWCC_I(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            IntWritable destCommunity) { 

        CommunityAggregatorData destCommData = 
            (CommunityAggregatorData) commAggMap.get(destCommunity);

        // Vertex statistics
        int d_in = getNumberOfEdgesToCommunity(vertex, destCommunity);
        int d_out = vertex.getNumEdges() - d_in;

        return computeApproximateWCC_I(d_in, d_out, destCommData);
    }


    /**
     * Calculates the approximation of the incremental improvement to global wcc
     * that would be incurred if this vertex moved to the destination community
     * with data destCommData. These equations are described in the SCC paper.
     */
    private double computeApproximateWCC_I(int d_in, int d_out, CommunityAggregatorData destCommData) {

        int r = destCommData.getSize();
        int b = destCommData.getNumberOfBorderEdges();
        double delta = destCommData.getEdgeDensity();

        double omega = globalCC;

        double q = (r == 0) ? 0.0 : (b - d_in)/((double) r);

        double theta_1_num = ((r - 1) * delta + 1 + q) * (d_in - 1) * delta;
        double theta_1_denom = (r + q) * ((r - 1) * (r - 2) * Math.pow(delta, 3) + (d_in - 1) * delta + q * (r - 1) * delta * omega + q * (q - 1) * omega + d_out * omega);
        double theta_1 = (theta_1_denom == 0) ? 0.0 : theta_1_num/theta_1_denom;

        double theta_2_num_left = -1 * (r - 1) * (r - 2) * Math.pow(delta, 3);
        double theta_2_num_right = (r - 1) * delta + q;
        double theta_2_denom_left = (r - 1) * (r - 2) * Math.pow(delta, 3) + q * (q - 1) * omega + q * (r - 1) * delta * omega;
        double theta_2_denom_right = (r + q) * (r - 1 + q);
        double theta_2_denom = theta_2_denom_left * theta_2_denom_right;
        double theta_2 = (theta_2_denom == 0) ? 0.0 : (theta_2_num_left * theta_2_num_right) / theta_2_denom;

        double theta_3_num_left = d_in * (d_in - 1) * delta;
        double theta_3_num_right = d_in + d_out;
        double theta_3_denom_left = d_in * (d_in - 1) * delta + d_out * (d_out - 1) * omega + d_out * d_in * omega;
        double theta_3_denom_right = r + d_out;
        double theta_3_denom = theta_3_denom_left * theta_3_denom_right;
        double theta_3 = (theta_3_denom == 0) ? 0.0 : (theta_3_num_left * theta_3_num_right) / theta_3_denom;

        double result = (d_in * theta_1 + (r - d_in) * theta_2 + theta_3);

        return result; 
    }

    /**
     * Given a destination community, counts and returns the number of edges go
     * from this vertex to this community
     */
    private int getNumberOfEdgesToCommunity(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            IntWritable destCommunity) {

        MapWritable ncm = vertex.getValue().getNeighborCommunityMap();
        int result = 0;
        for (Map.Entry<Writable, Writable> e : ncm.entrySet()) {
            IntWritable neighborComm = (IntWritable) e.getValue();
            if (neighborComm.equals(destCommunity)) result ++;
        }
        return result;
    }

    /**
     * Returns the set of all communities to which at least one neighbor
     * belongs. These are the possible communities to which the vertex may
     * transfer.
     */
    private Set<IntWritable> getCandidateCommunities(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex) {

        WccVertexData vData = vertex.getValue();
        MapWritable ncm = vData.getNeighborCommunityMap();

        // These commented lines can be used to ensure determinism, but
        // execution is much slower        
        // Set<IntWritable> candidates = new TreeSet(ncm.<IntWritable>values());
        //candidates.remove(new IntWritable(ISOLATED_COMMUNITY));
        //candidates.remove(new IntWritable(vData.getCommunity()));
       Set<IntWritable> candidates = new HashSet(ncm.<IntWritable>values());
       candidates.remove(new IntWritable(ISOLATED_COMMUNITY));
       candidates.remove(new IntWritable(vData.getCommunity()));
       return candidates;
    }
}

