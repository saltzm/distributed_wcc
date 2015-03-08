
package messages;

import utils.ArrayPrimitiveWritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.GenericWritable;
import java.util.Arrays;

public class WccMessage extends GenericWritable {
     
    private static Class[] CLASSES = {
        CommunityInitializationMessage.class,
        TransferMessage.class
    };

    //this empty initialize is required by Hadoop
    public WccMessage() {}

    public WccMessage(Writable instance) {
        set(instance);
    }

    @Override
    protected Class[] getTypes() {
        return CLASSES;
    }

    @Override
    public String toString() {
        return "WccMessage [getTypes()=" + Arrays.toString(getTypes()) + "]";
    }
}
