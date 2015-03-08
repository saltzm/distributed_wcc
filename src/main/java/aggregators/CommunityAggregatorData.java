
package aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class CommunityAggregatorData implements Writable {

    /**
     *  Community size 
     */
    private int size;

    /**
     *  Community edge density 
     */
    private int nInternalEdges;

    /**
     *  Number of border edges of the community
     */
    private int nBorderEdges; 

    // required by Hadoop 
    public CommunityAggregatorData() {
        this.size = 0;
        this.nInternalEdges = 0;
        this.nBorderEdges = 0;
    }

    public CommunityAggregatorData(int size, int nInternalEdges, 
            int nBorderEdges) {

        this.size = size;
        this.nInternalEdges = nInternalEdges;// * 2; //TODO mimick aggregating both sides...
        this.nBorderEdges = nBorderEdges;
    }

    public int getSize() { return size; }

    // This gets aggregated once from each node on the edge    
    public int getNumberOfInternalEdges() { return nInternalEdges; }
    public int getNumberOfInternalEdgesUndirected() { return nInternalEdges/2; }
    public int getNumberOfBorderEdges() { return nBorderEdges; }

    public double getEdgeDensity() {
        return (getSize() <= 1) ? 0.0 : // Less than can happen with hypothetically removing a vertex
            (double) getNumberOfInternalEdges() / (getSize() * (getSize() - 1));
    }

    public void aggregate(CommunityAggregatorData cad) {
        size += cad.getSize();
        nInternalEdges += cad.getNumberOfInternalEdges();
        nBorderEdges += cad.getNumberOfBorderEdges();
    }

    @Override 
    public void readFields(DataInput input) throws IOException {
        size = input.readInt();
        nInternalEdges = input.readInt();
        nBorderEdges = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(size);
        output.writeInt(nInternalEdges);
        output.writeInt(nBorderEdges);
    }

    @Override
    public String toString() {
        return "CommunityAggregatorData(size = " + getSize() + ", nInternalEdgesUndir = " + 
            getNumberOfInternalEdgesUndirected() + ", nBorderEdges = " +
            getNumberOfBorderEdges() + ", getEdgeDensity = " + getEdgeDensity() + ")";
    }
}
