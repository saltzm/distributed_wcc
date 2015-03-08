
package messages;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// TODO: Just use IntWritable ? 
public class TriangleCountMessage implements Writable {

    /**
     *  The number of triangles a vertex belongs to 
     */
    private int t;

    public TriangleCountMessage() {}

    public TriangleCountMessage(int t) {
        this.t = t;
    }

    public int getT() { return t; }

    public void setT(int t) { this.t = t; }

    @Override 
    public void readFields(DataInput input) throws IOException {
        t = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(t);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TriangleCountMessage) {
            TriangleCountMessage tcm = (TriangleCountMessage) o;
            return tcm.getT() == t;
        }
        return false;
    }

    @Override
    public String toString() {
        return "TriangleCountMessage(t = " + t + ")" ; 
    }
}
