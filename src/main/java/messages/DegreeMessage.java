
package messages;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DegreeMessage implements Writable {

    // TODO: REFACTOR
    /**
     *  The id of the vertex sending the message
     */
    private int sourceId;

    /**
     *  The degree of the vertex sending the message
     */
    private int degree;

    public DegreeMessage() {}

    public DegreeMessage(int sourceId, int degree) {
        this.sourceId = sourceId;
        this.degree = degree;
    }

    public int getSourceId() { return sourceId; }

    public int getDegree() { return degree; }

    @Override 
    public void readFields(DataInput input) throws IOException {
        sourceId = input.readInt();
        degree = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(sourceId);
        output.writeInt(degree);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DegreeMessage) {
            DegreeMessage dm = (DegreeMessage) o; 
            return dm.getSourceId() == sourceId &&
                   dm.getDegree() == degree;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 1 + sourceId*31 + degree*31;
    }

    @Override
    public String toString() {
        return "DegreeMessage(sourceId = " + sourceId + ", degree = " +
            degree +  ")" ; }
}
