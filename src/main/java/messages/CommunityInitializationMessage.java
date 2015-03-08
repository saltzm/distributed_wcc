
package messages;

import vertex.WccVertexData;

import org.apache.hadoop.io.Writable;
import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;
import java.lang.Comparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

public class CommunityInitializationMessage implements WritableComparable {
    private int id; 
    private int degree;
    private double clusteringCoefficient;
    //private boolean center;
    private int community;

    public CommunityInitializationMessage () {}

    public CommunityInitializationMessage (
            Vertex<IntWritable, WccVertexData, NullWritable> vertex) {
            //boolean center) { 
        this.id = vertex.getId().get();
        this.degree = vertex.getNumEdges();
        this.clusteringCoefficient = vertex.getValue().getClusteringCoefficient();
        //this.center = center;
        this.community = vertex.getValue().getCommunity();
    }  

    public CommunityInitializationMessage (int id, int degree, double cc, int comm) {
            //boolean center) { 
        this.id = id;
        this.degree = degree;
        this.clusteringCoefficient = cc;
        //this.center = center;
        this.community = comm;
    }  

    // Getters
    public int getId() { return id; }
    public int getDegree() { return degree; }
    public double getClusteringCoefficient() { return clusteringCoefficient; }
    public int getCommunity() { return community; }
    public boolean isCenter() { return id == community; }//center; }

//    // Setters
    public void setCommunity(int c) { this.community = c; }

    public CommunityInitializationMessage copy() {
        return new CommunityInitializationMessage(
            id, degree, clusteringCoefficient, community);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(degree);
        out.writeDouble(clusteringCoefficient);
        out.writeInt(community);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        degree = in.readInt();
        clusteringCoefficient = in.readDouble();
        community = in.readInt();
    }

    @Override
    public String toString() {
        return "(id = " + id + ", degree = " + degree + 
            " clusteringCoefficient = " + clusteringCoefficient + 
            " community: " + community + ", isCenter: " + isCenter() + ")"; 
    }

    @Override
    public int compareTo(Object m2obj) {
        CommunityInitializationMessage m2 = (CommunityInitializationMessage) m2obj;
        if (this.isCenter() && !m2.isCenter()) return 1; // center nodes are higher than border
        if (!this.isCenter() && m2.isCenter()) return -1;
        // both nodes are either borders or centers
        return compareToIgnoreCenter(m2obj);
    }

    @Override
    public boolean equals(Object other) {
        return compareTo(other) == 0;
    }

    @Override
    public int hashCode(){
        return id;
    }

    public int compareToIgnoreCenter(Object m2obj) {
        CommunityInitializationMessage m2 = (CommunityInitializationMessage) m2obj;
        double eps = 0.000001;
        if (this.clusteringCoefficient - m2.clusteringCoefficient > eps) return 1;
        if (this.clusteringCoefficient - m2.clusteringCoefficient < -eps) return -1;
        // cc's are equal
        if (this.degree > m2.degree) return 1;
        if (this.degree < m2.degree) return -1;
        // cc's and degrees are equal
        if (this.id > m2.id) return 1;
        if (this.id < m2.id) return -1; 

        return 0;
    }
}
