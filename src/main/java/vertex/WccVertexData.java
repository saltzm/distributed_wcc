
package vertex;

import messages.CommunityInitMessageArrayWritable;
import messages.CommunityInitializationMessage;
import computation.WccMasterCompute;
import utils.ArrayPrimitiveWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;

import java.lang.Math;

public class WccVertexData implements Writable {
    /**
     * The number of triangles x closes in the graph, i.e. t(x, V)
     */
    private int t;

    /**
     * The number of vertices that form at least one triangle with x, 
     * i.e. vt(x, V)
     */
    private int vt;

    private int communityT;

    private int communityVt;
   
    /**
     * The local clustering coefficient of x
     */
    private double clusteringCoefficient;

    /**
     * The identifier of the community to which the vertex beints
     */
    private int community;
    
    private int previousCommunity;

    private int bestCommunity;

    /**
     *  Used for community initialization phase
     */
    private CommunityInitMessageArrayWritable commInitNeighbors;

    /**
     *   A map from neighborId (IntWritable) to its community (IntWritable)
     */
    private MapWritable neighborCommunityMap;

    /**
     *   An array containing the vertex's neighbors that have a higher degree.
     *   Used for preprocessing
     */
    private ArrayPrimitiveWritable higherDegreeNeighbors;

    /**
     *   An array containing the vertex's neighbors
     *   Used for preprocessing
     */
    private ArrayPrimitiveWritable neighbors;

    public WccVertexData () {
        t = vt = communityT = communityVt = 0;
        community = previousCommunity = bestCommunity = WccMasterCompute.ISOLATED_COMMUNITY;
        commInitNeighbors = new CommunityInitMessageArrayWritable(new CommunityInitializationMessage[0]);
        neighborCommunityMap = new MapWritable();
        higherDegreeNeighbors = new ArrayPrimitiveWritable(new int[0]); 
        neighbors = new ArrayPrimitiveWritable(new int[0]); 
    }

    public int getT() { return t; }
    public int getVt() { return vt; }
    public int getCommunityT() { return communityT; }
    public int getCommunityVt() { return communityVt; }
    public double getClusteringCoefficient() { return clusteringCoefficient; }
    public int getCommunity() { return community; }
    public int getBestCommunity() { return bestCommunity; }

    public CommunityInitMessageArrayWritable getCommInitNeighbors() { 
        return commInitNeighbors;
    }

    public MapWritable getNeighborCommunityMap() { return neighborCommunityMap; }

    public ArrayPrimitiveWritable getHigherDegreeNeighbors() {
        return higherDegreeNeighbors;
    }

    public ArrayPrimitiveWritable getNeighbors() {
        return neighbors;
    }

    public void updateCommunity(int c) { 
        this.previousCommunity = this.community;
        this.community = c; 
    }

    public void saveCurrentCommunityAsBest() {
        bestCommunity = community;
    }

    public void savePreviousCommunityAsBest() {
        bestCommunity = previousCommunity;
    }

    public void setT(int t) { this.t = t; }

    public void setVt(int vt) { this.vt = vt; }

    public void setCommunityT(int communityT) { this.communityT = communityT; }

    public void setCommunityVt(int communityVt) { this.communityVt = communityVt; }


    // TODO: SHOULD ONLY BE USED FOR TESTING
    public void setCommunity(int community) {
        this.community = community;
    }

    // TODO: SHOULD ONLY BE USED FOR TESTING
    public void setBestCommunity(int bestCommunity) {
        this.bestCommunity = bestCommunity;
    }

    public void setClusteringCoefficient(double cc) { 
        this.clusteringCoefficient = cc; 
    }

    public void setCommInitNeighbors(CommunityInitMessageArrayWritable sns) { 
        this.commInitNeighbors = sns; 
    }

    public void setNeighborCommunityMap(MapWritable ncm) { 
        this.neighborCommunityMap = ncm;
    }
    
    public void setHigherDegreeNeighbors(ArrayPrimitiveWritable hdns) {
        this.higherDegreeNeighbors = hdns;
    }

    public void setNeighbors(ArrayPrimitiveWritable ns) {
        this.neighbors = ns;
    }

    @Override 
    public void readFields(DataInput input) throws IOException {
        t                       = input.readInt();
        vt                      = input.readInt();
        communityT              = input.readInt();
        communityVt             = input.readInt();
        clusteringCoefficient   = input.readDouble();
        community               = input.readInt();
        previousCommunity       = input.readInt();
        bestCommunity           = input.readInt();
        commInitNeighbors.readFields(input);
        neighborCommunityMap.readFields(input);
        higherDegreeNeighbors.readFields(input); 
        neighbors.readFields(input); 
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(t);
        output.writeInt(vt);
        output.writeInt(communityT);
        output.writeInt(communityVt);
        output.writeDouble(clusteringCoefficient);
        output.writeInt(community);
        output.writeInt(previousCommunity);
        output.writeInt(bestCommunity);
        commInitNeighbors.write(output); 
        neighborCommunityMap.write(output); 
        higherDegreeNeighbors.write(output);
        neighbors.write(output);
    }

    @Override
    public String toString() {
//        return "(t = " + t + ", vt = " + vt + ", clusteringCoefficient = " +
//            clusteringCoefficient + ", community = " + community + ", bestCommunity = " + bestCommunity + ")" ;
        return bestCommunity + "";
    }

//    @Override
//    public boolean equals(Object o) {
//        if (o instanceof WccVertexData) {
//            WccVertexData other = (WccVertexData) o;
//            double epsilon = 0.00000001;
//            return 
//                other.getT() == this.getT() &&
//                other.getVt() == this.getVt() &&
//                Math.abs(other.getClusteringCoefficient() -
//                        this.getClusteringCoefficient()) < epsilon  &&
//                other.getCommunity() == this.getCommunity();
//        }
//        return false;
//    }
//
    @Override
    public int hashCode() {
        return 31*t + 31*vt + 31*community; 
    }
}
