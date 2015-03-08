
package combiner;

import messages.TriangleCountMessage;

import org.apache.giraph.combiner.MessageCombiner;

import org.apache.hadoop.io.IntWritable;

/**
 *  * MessageCombiner which sums up {@link TriangleCountMessage} message values.
 *   */
public class TriangleCountCombiner
    extends MessageCombiner<IntWritable, TriangleCountMessage> {

    @Override
    public void combine(IntWritable vertexIndex, 
            TriangleCountMessage originalMessage,
            TriangleCountMessage messageToCombine) {
        originalMessage.setT(originalMessage.getT() + messageToCombine.getT()); 
    }

    @Override 
    public TriangleCountMessage createInitialMessage() {
        return new TriangleCountMessage(0);
    }
}
