
package aggregators;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import java.util.Map;

public class CommunityAggregator extends BasicAggregator<MapWritable> {

  @Override
  public void aggregate(MapWritable m) {
    if (!m.isEmpty()) {
      // Get current aggregated map
      MapWritable current = getAggregatedValue();
      // For every comm id -> comm agg data tuple in the message, 
      // aggregate that data with existing data for comm id in the
      // current map or add the new community and data to the map
      for (Map.Entry<Writable, Writable> e : m.entrySet()) {
        IntWritable commId = (IntWritable) e.getKey();
        CommunityAggregatorData cad = (CommunityAggregatorData) e.getValue();
        if (!current.containsKey(commId)) {
          current.put(commId, cad); 
        } else {
          CommunityAggregatorData currentCommData = 
            (CommunityAggregatorData) current.get(commId);
          currentCommData.aggregate(cad);
        }
      }
    } 
  }

  @Override
  public MapWritable createInitialValue() {
    return new MapWritable();
  }
}
