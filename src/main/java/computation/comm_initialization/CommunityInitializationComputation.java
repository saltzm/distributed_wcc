
package computation.comm_initialization;
import computation.WccMasterCompute;

import messages.CommunityInitializationMessage;
import messages.CommunityInitMessageArrayWritable;
import messages.WccMessage;
import messages.TransferMessage;
import vertex.WccVertexData;

import java.util.Iterator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.graph.BasicComputation;


//TODO: Potentially separate into two so it can use diff input and output types
public class CommunityInitializationComputation 
    extends BasicComputation<IntWritable, WccVertexData, NullWritable,
            WccMessage>{

    private int currentPhase;
    public void preSuperstep() {
        currentPhase = ((IntWritable) getAggregatedValue(WccMasterCompute.PHASE)).get();
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
            Iterable<WccMessage> messages) {

        boolean communityChanged = 
          (currentPhase == WccMasterCompute.COMMUNITY_INITIALIZATION_START) ?
            initializeCommunity(vertex, messages) : 
            updateInitialCommunity(vertex, messages);

        boolean neighborsNotified = notifyNeighbors(vertex, communityChanged);
        if (neighborsNotified && 
            currentPhase == WccMasterCompute.COMMUNITY_INITIALIZATION_IN_PROCESS) {
          aggregate(WccMasterCompute.REPEAT_PHASE, new BooleanWritable(true));
        }
    }
 
    public static boolean initializeCommunity(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            Iterable<WccMessage> messages) {

        // Initialize data structure containing the information of all the nodes
        // neighbors: id, clustering coefficient, degree, and whether or not the
        // neighbor is a center of its own community
        CommunityInitializationMessage[] commInitNeighbors = 
            getInitialCommunityInitializationNeighbors(messages);

        // The current vertex data
        WccVertexData vData = vertex.getValue();

        // Update the WccVertexData data structure to contain this new structure
        vData.setCommInitNeighbors(
            new CommunityInitMessageArrayWritable(commInitNeighbors));

        // Change current vertex's community
        return enactCommunityChange(vertex, commInitNeighbors);
    }

    public static boolean updateInitialCommunity(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            Iterable<WccMessage> messages) {

        // The current vertex data
        WccVertexData vData = vertex.getValue();

//        CommunityInitializationMessage[] commInitNeighbors = 
//            (CommunityInitializationMessage[]) vData.getCommInitNeighbors().get();

        Writable[] commInitNeighbors = 
            vData.getCommInitNeighbors().get();
        // Update commInitNeighbors data structure based on ChangedMessages
        updateCommInitNeighbors(vertex, messages, commInitNeighbors);

        // Change current vertex's community
        return enactCommunityChange(vertex, commInitNeighbors);
    }

    /******************************************************************
     * -----------------------HELPER METHODS--------------------------*
     *****************************************************************/
    
    /**
     * Update commInitNeighbors data structure based on ChangedMessages
     */
    private static void updateCommInitNeighbors(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            Iterable<WccMessage> messages,
//            CommunityInitializationMessage[] commInitNeighbors) {
            Writable[] commInitNeighbors) {
        
        
        // build neighbor map with id as key (from commInitNeighbors)
        HashMap<Integer, CommunityInitializationMessage> commInitNeighborsMap = 
            new HashMap(commInitNeighbors.length);
        //for (CommunityInitializationMessage m : commInitNeighbors) {
        //TODO: Just have this use a map writable
        for (Writable m : commInitNeighbors) {
            CommunityInitializationMessage cim = (CommunityInitializationMessage) m;
            commInitNeighborsMap.put(cim.getId(), cim);
        }


        // For each TransferMessage, get its id, get the
        // CommunityInitializationMessage from the map corresponding to that id,
        // remove that object from commInitNeighbors, flip the 'center' boolean,
        // and add it back to commInitNeighbors
        int count = 0;
        for (WccMessage m : messages) {
            TransferMessage tm = (TransferMessage) m.get();
            int changedId = tm.getSourceId();
            CommunityInitializationMessage cimToChange = commInitNeighborsMap.get(changedId);
            count ++;
            if (cimToChange == null) {
                System.out.println("count: " + count);
            }
            cimToChange.setCommunity(tm.getCommunity());
        }
    }

    /**
     *  Find the neighbor with the max value according to the compareTo Function
     *  of CommunityInitializationMessage 
     */
    private static CommunityInitializationMessage getHighestNeighbor(
                //CommunityInitializationMessage[] commInitNeighbors) {
                Writable[] commInitNeighbors) {
        //CommunityInitializationMessage max = commInitNeighbors[0];
        CommunityInitializationMessage max = (CommunityInitializationMessage) commInitNeighbors[0];
        for (int i = 1; i < commInitNeighbors.length; i++) {
            CommunityInitializationMessage cur = (CommunityInitializationMessage) commInitNeighbors[i];
            if (cur.compareTo(max) > 0) 
                max = cur; 
        }
        return max;
    }

    /** Initialize data structure containing the information of all the nodes 
     *  neighbors: id, clustering coefficient, degree, and whether or not the 
     *  neighbor is a center of its own community
     */
    private static CommunityInitializationMessage[] getInitialCommunityInitializationNeighbors (
            Iterable<WccMessage> messages) {

        LinkedList<CommunityInitializationMessage> nList = new LinkedList();
        for (WccMessage m : messages) {
            nList.addFirst((CommunityInitializationMessage) m.get());
        }

        return nList.toArray(new CommunityInitializationMessage[0]);
    }

    /**
     * Based on the neighbors' information, decides whether or not to update
     * its own community, and if so, updates the value in vData accordingly.
     * Returns true if the value of its community changed such that it
     * changed from being the center of its own community to being the
     * border of another community, or vice versa, but not if it goes from
     * being the border of one community to being the border of another
     */
    private static boolean enactCommunityChange(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            //CommunityInitializationMessage[] commInitNeighbors) {
            Writable[] commInitNeighbors) {
        
        WccVertexData vData = vertex.getValue();

        CommunityInitializationMessage highestNeighbor = getHighestNeighbor(commInitNeighbors);

        // Used for comparison purposes
        CommunityInitializationMessage thisDataMessage = new CommunityInitializationMessage(vertex);

        boolean changed = false;

        // If highest neighbor is higher than us and a center:
        if (highestNeighbor.isCenter() &&
                highestNeighbor.compareToIgnoreCenter(thisDataMessage) > 0) {
            // If we're already a member of the neighbor's community, don't change
            // Else become a member of that neighbor's community
            if (vData.getCommunity() != highestNeighbor.getId()) {
                // Only send a changed message if it's changed from a center to
                // a border, not if it changes from a border to a border
                if (thisDataMessage.isCenter()) { changed = true; }
                vData.updateCommunity(highestNeighbor.getId());
            } 
        } else { // We need to be the center of our own community
            // If we already are, then nothing changes
            // Else, become the center of our own community
            if (vData.getCommunity() != vertex.getId().get()) {
                vData.updateCommunity(vertex.getId().get());
                changed = true;
            }
        }
        return changed;
    }

    /**
     * Returns true if any neighbors are notified of a change
     */
    private boolean notifyNeighbors(///*Iterator<IntWritable>*/ getNeighborsToNotify(
            Vertex<IntWritable, WccVertexData, NullWritable> vertex,
            boolean communityChanged) {

        boolean neighborsNotified = false;
        //CommunityInitializationMessage[] commInitNeighbors =
        //    (CommunityInitializationMessage[]) vertex.getValue().getCommInitNeighbors().get();

        Writable[] commInitNeighbors =
            vertex.getValue().getCommInitNeighbors().get();
        if (communityChanged) {
            CommunityInitializationMessage thisDataMessage = 
                new CommunityInitializationMessage(vertex);
            // Notify all neighbors with values less than us, because these are
            // the only other vertices that could decide to become a member of
            // our community if we were a center
            IntWritable tempDest = new IntWritable();
            //for (CommunityInitializationMessage m : commInitNeighbors) {
            for (Writable m : commInitNeighbors) {
                CommunityInitializationMessage cim = (CommunityInitializationMessage) m;
                if (cim.compareToIgnoreCenter(thisDataMessage) < 0) {
                    neighborsNotified = true;
                    tempDest.set(cim.getId());
                    sendMessage(tempDest, 
                        new WccMessage(new TransferMessage(vertex.getId().get(),
                            vertex.getValue().getCommunity())));
                }
            }
        } 
        return neighborsNotified;
    }
}
