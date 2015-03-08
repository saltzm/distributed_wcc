
package messages;

import utils.ArrayPrimitiveWritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AdjacencyListMessage implements Writable {

    /**
     *  The id of the vertex sending the message
     */
    private int sourceId;

    /**
     *  The adjList that the vertex transferred to
     */
    private ArrayPrimitiveWritable adjList;

    public AdjacencyListMessage() {
        adjList = new ArrayPrimitiveWritable(new int[0]);
    }

    public AdjacencyListMessage(int sourceId, ArrayPrimitiveWritable adjList) {
        this.sourceId = sourceId;
        this.adjList = adjList;
    }

    public int getSourceId() { return sourceId; }

    public int[] getAdjacencyList() { return (int[]) adjList.get(); }

    @Override 
    public void readFields(DataInput input) throws IOException {
        sourceId = input.readInt();
        adjList.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(sourceId);
        adjList.write(output);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AdjacencyListMessage) {
            AdjacencyListMessage alm = (AdjacencyListMessage) o;
            return alm.getSourceId() == sourceId &&
                   alm.getAdjacencyList().equals(adjList);
        } else return false;
    }

    @Override
    public int hashCode() { return sourceId; }

    @Override
    public String toString() {
        return "AdjacencyListMessage(sourceId = " + sourceId + ", adjList = " +
            adjList +  ")" ; }
}
