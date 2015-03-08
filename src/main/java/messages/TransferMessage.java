
package messages;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransferMessage implements Writable {

    /**
     *  The id of the vertex sending the message
     */
    private int sourceId;

    /**
     *  The community that the vertex transferred to
     */
    private int community;

    public TransferMessage() {}

    public TransferMessage(int sourceId, int community) {
        this.sourceId = sourceId;
        this.community = community;
    }

    public int getSourceId() { return sourceId; }

    public int getCommunity() { return community; }

    @Override 
    public void readFields(DataInput input) throws IOException {
        sourceId = input.readInt();
        community = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(sourceId);
        output.writeInt(community);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof TransferMessage) {
            TransferMessage m = (TransferMessage) other;
            return m.getSourceId() == sourceId && 
                   m.getCommunity() == community;
        } else return false;
    }

    @Override
    public int hashCode() {
        return 1 + sourceId*31 + community*31;
    }

    @Override
    public String toString() {
        return "TransferMessage(sourceId = " + sourceId + ", community = " +
            community +  ")" ; }
}
