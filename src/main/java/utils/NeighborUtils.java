
package utils;

import vertex.WccVertexData;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.util.HashSet;
import java.util.Set;
import java.util.Map;

public class NeighborUtils {
    // Do not instantiate
    private NeighborUtils() {}

   /**
     * Gets an array of IntWritables to be passed in to the
     * PreprocessingMessage constructor. TODO: for now uses the first element to
     * be contain the source vertex id. Later on should just modify the
     * PreprocessingMessage class
     * @param vertex Vertex
     * @return An array containing the ids of the vertex's neighbors
     */
    public static ArrayPrimitiveWritable getNeighborsWritableArray(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex) {

        int[] neighbors = new int[vertex.getNumEdges()];

        int i = 0;
        for (Edge<IntWritable, NullWritable> e : vertex.getEdges()) {
            neighbors[i] = e.getTargetVertexId().get();
            i++;
        }
        return new ArrayPrimitiveWritable(neighbors);
    }

    public static int countCommonNeighbors(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            int[] neighborNeighbors) {

        int numCommonNeighbors = 0;
        IntWritable nn = new IntWritable();
        for (int i = 0; i < neighborNeighbors.length; i++) {
            nn.set(neighborNeighbors[i]);
            if (vertex.getEdgeValue(nn) != null) {
                numCommonNeighbors++;
            }
        }
        return numCommonNeighbors;
    }

    /**
     *  Use the neighborCommunityMap of the vertex to build a list of its neighbors
     *  in the same community as the vertex and wrap it in an
     *  ArrayPrimitiveWritable
     */
    public static ArrayPrimitiveWritable getNeighborsInCommunity(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex) {

        WccVertexData vData = vertex.getValue();
        MapWritable ncm = vData.getNeighborCommunityMap();

        // Count neighbors in community
        int numNeighborsInCommunity = 0;
        for(Map.Entry<Writable, Writable> entry : ncm.entrySet()) {
            int neighborCommunity = ((IntWritable) entry.getValue()).get();
            if (neighborCommunity == vData.getCommunity()) {
                numNeighborsInCommunity++;
            }
        }

        int[] neighborsInCommunity = new int[numNeighborsInCommunity];

        int i = 0;
        for(Map.Entry<Writable, Writable> entry : ncm.entrySet()) {
            int neighborCommunity = ((IntWritable) entry.getValue()).get();
            if (neighborCommunity == vData.getCommunity()) {
                IntWritable id = (IntWritable) entry.getKey();
                neighborsInCommunity[i] = id.get();
                i++;
            }
        }
        return new ArrayPrimitiveWritable(neighborsInCommunity);
    }

}
