
package messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class CommunityAggregatorMessage implements Writable {

    /**
     *  Used to identify the community
     */
    private int communityId;

    /**
     *  Used to calculate edge density of a community
     */
    private int numberOfEdgesInCommunity;

    /**
     *  Used to calculate number of border edges of a community
     */
    private int numberOfEdgesNotInCommunity;

    // required by Hadoop 
    public CommunityAggregatorMessage() {}

    public CommunityAggregatorMessage(int communityId, 
            int numberOfEdgesInCommunity, int numberOfEdgesNotInCommunity) {

        this.communityId = communityId;
        this.numberOfEdgesInCommunity = numberOfEdgesInCommunity;
        this.numberOfEdgesNotInCommunity = numberOfEdgesNotInCommunity;
    }

    public int getCommunityId() { return communityId; }
    public int getNumberOfEdgesInCommunity() { return numberOfEdgesInCommunity; }
    public int getNumberOfEdgesNotInCommunity() { return numberOfEdgesNotInCommunity; }

    @Override 
    public void readFields(DataInput input) throws IOException {
        communityId = input.readInt();
        numberOfEdgesInCommunity = input.readInt();
        numberOfEdgesNotInCommunity = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(communityId);
        output.writeInt(numberOfEdgesInCommunity);
        output.writeInt(numberOfEdgesNotInCommunity);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CommunityAggregatorMessage) {
            CommunityAggregatorMessage cam = (CommunityAggregatorMessage) o;
            return cam.getCommunityId() == communityId &&
                   cam.getNumberOfEdgesInCommunity() == numberOfEdgesInCommunity &&
                   cam.getNumberOfEdgesNotInCommunity() == numberOfEdgesNotInCommunity;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 1 + 31*(communityId + numberOfEdgesInCommunity + numberOfEdgesNotInCommunity);
    }


    @Override
    public String toString() {
        return "(communityId = " + communityId + ", numberOfEdgesInCommunity = " + 
            numberOfEdgesInCommunity + ", numberOfEdgesNotInCommunity = " +
            numberOfEdgesNotInCommunity + ")" ;
    }
}
