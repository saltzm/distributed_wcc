/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils;


import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import org.apache.giraph.edge.ReuseObjectsOutEdges;
import org.apache.giraph.edge.MutableOutEdges;
import org.apache.giraph.edge.StrictRandomAccessOutEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.ReusableEdge;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.giraph.utils.Trimmable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;


/**
 * This class was copied from the LongNullHashSetEdges class in the giraph
 * project and modified to use IntWritables instead of LongWritables.
 *
 * {@link OutEdges} implementation with int ids and null edge values,
 * backed by a {@link LongOpenHashSet}.
 * Parallel edges are not allowed.
 * Note: this implementation is optimized for fast random access and mutations,
 * and uses less space than a generic {@link HashMapEdges}  
 * */
public class IntNullHashSetEdges
    implements ReuseObjectsOutEdges<IntWritable, NullWritable>,
    MutableOutEdges<IntWritable, NullWritable>,
    StrictRandomAccessOutEdges<IntWritable, NullWritable>,
    Trimmable {
  /** Hash set of target vertex ids. */
  private IntOpenHashSet neighbors;

  @Override
  public void initialize(Iterable<Edge<IntWritable, NullWritable>> edges) {
    EdgeIterables.initialize(this, edges);
  }

  @Override
  public void initialize(int capacity) {
    neighbors = new IntOpenHashSet(capacity);
  }

  @Override
  public void initialize() {
    neighbors = new IntOpenHashSet();
  }

  @Override
  public void add(Edge<IntWritable, NullWritable> edge) {
    neighbors.add(edge.getTargetVertexId().get());
  }

  @Override
  public void remove(IntWritable targetVertexId) {
    neighbors.remove(targetVertexId.get());
  }

  @Override
  public int size() {
    return neighbors.size();
  }

  @Override
  public Iterator<Edge<IntWritable, NullWritable>> iterator() {
    // Returns an iterator that reuses objects.
    // The downcast is fine because all concrete Edge implementations are
    // mutable, but we only expose the mutation functionality when appropriate.
    return (Iterator) mutableIterator();
  }

  @Override
  public Iterator<MutableEdge<IntWritable, NullWritable>> mutableIterator() {
    return new Iterator<MutableEdge<IntWritable, NullWritable>>() {
      /** Wrapped neighbors iterator. */
      private IntIterator neighborsIt = neighbors.iterator();
      /** Representative edge object. */
      private ReusableEdge<IntWritable, NullWritable> representativeEdge =
          EdgeFactory.createReusable(new IntWritable());

      public boolean hasNext() {
        return neighborsIt.hasNext();
      }

      @Override
      public MutableEdge<IntWritable, NullWritable> next() {
        representativeEdge.getTargetVertexId().set(neighborsIt.nextInt());
        return representativeEdge;
      }

      @Override
      public void remove() {
        neighborsIt.remove();
      }
    };
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(neighbors.size());
    IntIterator neighborsIt = neighbors.iterator();
    while (neighborsIt.hasNext()) {
      out.writeInt(neighborsIt.nextInt());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEdges = in.readInt();
    initialize(numEdges);
    for (int i = 0; i < numEdges; ++i) {
      neighbors.add(in.readInt());
    }
  }

  @Override
  public NullWritable getEdgeValue(IntWritable targetVertexId) {
    if (neighbors.contains(targetVertexId.get())) {
      return NullWritable.get();
    } else {
      return null;
    }
  }

  @Override
  public void setEdgeValue(IntWritable targetVertexId,
    NullWritable edgeValue) {
    // No operation.
    // Only set value for an existing edge.
    // If the edge exist, the Null value is already there.
  }

  @Override
  public void trim() {
    neighbors.trim();
  }
}
