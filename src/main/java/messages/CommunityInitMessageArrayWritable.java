
package messages;

import org.apache.hadoop.io.ArrayWritable;

/**
 *  
 */
public class CommunityInitMessageArrayWritable extends ArrayWritable {
    public CommunityInitMessageArrayWritable() {
        super(CommunityInitializationMessage.class); 
    }
    public CommunityInitMessageArrayWritable(CommunityInitializationMessage[] neighbors) { 
        super(CommunityInitializationMessage.class); 
        super.set(neighbors);
    }
}
