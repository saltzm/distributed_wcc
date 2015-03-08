
package computation;

import aggregators.*;
import computation.comm_initialization.*;
import computation.preprocessing.*;
import computation.wcc_iteration.*;
import combiner.TriangleCountCombiner;

import org.apache.giraph.master.DefaultMasterCompute;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.LongMinAggregator;
import org.apache.giraph.aggregators.LongMaxAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.DoubleOverwriteAggregator;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.LongOverwriteAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.BooleanAndAggregator;
import org.apache.giraph.aggregators.BooleanOverwriteAggregator;
import org.apache.giraph.aggregators.IntMinAggregator;
import org.apache.giraph.aggregators.IntMaxAggregator;

import org.apache.giraph.combiner.SimpleSumMessageCombiner;

import org.apache.giraph.utils.MemoryUtils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BooleanWritable;


public class WccMasterCompute extends DefaultMasterCompute {
  /*******************************************************
   *                  AGGREGATOR NAMES                   *
   ******************************************************/
  public static final String GRAPH_CLUSTERING_COEFFICIENT =
    "graph-clustering-coefficient";
  public static final String MAX_DEGREE = "max-degree";
  public static final String PREPROCESSING_VERTICES_SENT = "preprocessing-vertices-sent";
  public static final String MIN_MEMORY_AVAILABLE = "min-memory-available";

  // Used when a phase is repeated and needs to know how many repetitions have
  // happened so far
  public static final String INTERPHASE_STEP = "interphase-step";

  public static final String COMMUNITY_AGGREGATES = "community-aggregates";

  // Phase control
  public static final String PHASE = "phase";
  public static final String REPEAT_PHASE = "repeat-phase";
  public static final String NEXT_PHASE = "next-phase";
  // True only if a vertex is manually picking next phase
  public static final String PHASE_OVERRIDE = "phase-override"; 
  public static final String NUMBER_OF_PREPROCESSING_STEPS = "number-of-preprocessing-steps";
  public static final String NUMBER_OF_WCC_STEPS = "number-of-wcc-steps";

  // WCC computation
  public static final String WCC = "wcc";
  public static final String BEST_WCC = "best-wcc";

  // Related to termination conditions
  public static final String RETRIES_LEFT = "retries-left";
  public static final String FOUND_NEW_BEST_PARTITION = "found-new-best-partition";

  /*******************************************************
   *                      PHASES                         *
   ******************************************************/
  public static final int INPUT = -1;
  public static final int START = 0;
  public static final int PREPROCESSING_START = 1;
  public static final int SENDING_ADJACENCY_LISTS = 2;
  public static final int COUNTING_TRIANGLES = 3;
  public static final int FINISH_PREPROCESSING = 4;
  public static final int COMMUNITY_INITIALIZATION_START = 5;
  public static final int COMMUNITY_INITIALIZATION_IN_PROCESS = 6;
  public static final int START_WCC_ITERATION = 7;
  public static final int UPDATE_COMMUNITY_INFO = 8;
  public static final int COMPUTE_WCC = 9;
  public static final int CHOOSE_NEXT_COMMUNITY = 10;

  /*******************************************************
   *               COMPUTATION-RELATED                   *
   ******************************************************/
  public static final int ISOLATED_COMMUNITY = -1; 
  
  /*******************************************************
   *               CONFIGURATION-RELATED                   *
   ******************************************************/

  public static final String NUMBER_OF_PREPROCESSING_STEPS_CONF_OPT = "wcc.numPreprocessingSteps";
  public static final int DEFAULT_NUMBER_OF_PREPROCESSING_STEPS = 1;

  public static final String NUMBER_OF_WCC_COMPUTATION_STEPS_CONF_OPT = "wcc.numWccComputationSteps";
  public static final int DEFAULT_NUMBER_OF_WCC_COMPUTATION_STEPS = 1;

  // arnau = 7 -> me = 5
  public static final String MAX_RETRIES_CONF_OPT = "wcc.maxRetries";
  public static final int DEFAULT_MAX_RETRIES = 2; 
  private static int maxRetries;

  @Override
  public void compute() {
    printMetrics();
    printHeader();

    int retriesLeft = this.<IntWritable>getAggregatedValue(RETRIES_LEFT).get();
    IntWritable phaseAgg = getAggregatedValue(PHASE);

    int previousPhase = (getSuperstep() == 0) ? INPUT : phaseAgg.get(); 
    boolean repeatPhase = this.<BooleanWritable>getAggregatedValue(REPEAT_PHASE).get();
    int nextPhase = getPhaseAfter(previousPhase, repeatPhase); 
    setAggregatedValue(PHASE, new IntWritable(nextPhase));

    switch (nextPhase) {
      case START:
        System.out.println("Phase: Start");
        setComputation(StartComputation.class);
        setAggregatedValue(RETRIES_LEFT, new IntWritable(maxRetries));
      break;

      case PREPROCESSING_START:
        System.out.println("Phase: Preprocessing Start");
        setComputation(StartPreprocessingComputation.class);
        System.out.println("Total number of edges before preprocessing: " +
            getTotalNumEdges()/2.0);
        long maxDegree = this.<LongWritable>getAggregatedValue(MAX_DEGREE).get();
        System.out.println("Max degree: " + maxDegree);
      break;

      case SENDING_ADJACENCY_LISTS:
        System.out.println("Phase: Preprocessing (Sending Adjacency Lists)");
        setComputation(SendAdjacencyListComputation.class);
        setMessageCombiner(null);
        if (previousPhase == PREPROCESSING_START) {
          LongWritable preprocessingVerticesSent =
              this.<LongWritable>getAggregatedValue(PREPROCESSING_VERTICES_SENT);
          System.out.println("Total number of vertices sent in adjacency lists " + 
              "during preprocessing: " + preprocessingVerticesSent);
          int nWorkers = getConf().getMinWorkers();
          double minMemAvailable = this.<DoubleWritable>getAggregatedValue(MIN_MEMORY_AVAILABLE).get();
          //double maxMem = minMemAvailable * Math.pow(10, 9); 
          double maxMem = 10 * Math.pow(10, 9); 
          int numPrepSteps = (int) Math.ceil((double) preprocessingVerticesSent.get() / (nWorkers * maxMem / 8));
          //int numPrepSteps = 5;
          System.out.println("Min workers: " + nWorkers);
          System.out.println("Number of preprocessing steps: " + numPrepSteps);
          setAggregatedValue(NUMBER_OF_PREPROCESSING_STEPS, new IntWritable(numPrepSteps));
          setAggregatedValue(NUMBER_OF_WCC_STEPS, new IntWritable(numPrepSteps));
        } else {
          IntWritable interphaseStepAgg = getAggregatedValue(INTERPHASE_STEP);
          setAggregatedValue(INTERPHASE_STEP, new IntWritable(interphaseStepAgg.get() + 1));
        }
      break;

      case COUNTING_TRIANGLES:
        System.out.println("Phase: Preprocessing (Counting Triangles)");
        setComputation(CountTrianglesComputation.class);
        setMessageCombiner(TriangleCountCombiner.class);
      break;

      case FINISH_PREPROCESSING:
        System.out.println("Phase: Finish Preprocessing");
        setComputation(FinishPreprocessingComputation.class);
      break;

      case COMMUNITY_INITIALIZATION_START:
        System.out.println("Phase: Community Initialization Start");
        System.out.println("Total number of edges after preprocessing: " +
          getTotalNumEdges()/2.0);
        
        double graphClusteringCoefficient =
            this.<DoubleWritable>getAggregatedValue(GRAPH_CLUSTERING_COEFFICIENT).get();

        System.out.println("Graph clustering coefficient: " +
                graphClusteringCoefficient / getTotalNumVertices()); 

        setComputation(CommunityInitializationComputation.class);
      break;

      case COMMUNITY_INITIALIZATION_IN_PROCESS:
        System.out.println("Phase: Community Initialization In Process");
      break;

      case START_WCC_ITERATION:
        System.out.println("Phase: WCC Iteration Start");
        setComputation(StartWccIterationComputation.class);
      break;

      case UPDATE_COMMUNITY_INFO:
        System.out.println("Phase: Updating Community Information");
        setComputation(UpdateCommunityInfoComputation.class);
      break;

      case COMPUTE_WCC:
        // Before starting wcc computation again, check for termination
        // If retriesLeft == 0, terminate; otherwise, continue computation
        if (retriesLeft == 0) {
          // TODO: ensure this is the correct WCC for the output results even if
          // it happens on the last one 
          double best_wcc = this.<DoubleWritable>getAggregatedValue(BEST_WCC).get();
          System.out.println("Computation finished.");
          System.out.println("Final WCC: " + best_wcc);
          haltComputation();
        } else { // continue computation
          System.out.println("Phase: Computing WCC");

          // If it's about to be the first phase of wcc computation
          if (previousPhase == UPDATE_COMMUNITY_INFO) {
              setComputation(ComputeWccComputation.class);
              setAggregatedValue(INTERPHASE_STEP, new IntWritable(0));
          } else { // In the middle of iteration
              IntWritable interphaseStepAgg = getAggregatedValue(INTERPHASE_STEP);
              setAggregatedValue(INTERPHASE_STEP, new IntWritable(interphaseStepAgg.get() + 1));
          }
        }
      break;

      case CHOOSE_NEXT_COMMUNITY:
        System.out.println("Phase: Choosing new communities");
        setAggregatedValue(RETRIES_LEFT, new IntWritable(retriesLeft - 1));
        setComputation(ChooseNextCommunityComputation.class);
      break;
    }

    // Just finished aggregating latest wcc - must check for improvement
    if (previousPhase == CHOOSE_NEXT_COMMUNITY) {
      double wcc = this.<DoubleWritable>getAggregatedValue(WCC).get();
      double best_wcc = this.<DoubleWritable>getAggregatedValue(BEST_WCC).get();
      double new_wcc = wcc/getTotalNumVertices();

      // First time is just after initialization; no transfers yet
      System.out.println("New WCC: " + new_wcc);
      System.out.println("Best Prior WCC: " + best_wcc);
      System.out.println("Retries left: " + retriesLeft);

      // If wcc has improved
      if (new_wcc - best_wcc > 0.0) { 
        System.out.println("Found new best");
        setAggregatedValue(BEST_WCC, new DoubleWritable(new_wcc));
        setAggregatedValue(FOUND_NEW_BEST_PARTITION, new BooleanWritable(true));

        // If wcc has changed by more than 1%
        if ((new_wcc - best_wcc) / best_wcc > 0.01) { 
          System.out.println("Resetting retries");
          setAggregatedValue(RETRIES_LEFT, new IntWritable(maxRetries));
        }
      }
      System.out.println();
    }
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    // TODO: split
    registerAggregator(COMMUNITY_AGGREGATES, CommunityAggregator.class);
    registerAggregator(WCC, DoubleSumAggregator.class);
    registerAggregator(FOUND_NEW_BEST_PARTITION, BooleanOverwriteAggregator.class);
    registerAggregator(REPEAT_PHASE, BooleanOrAggregator.class);
    registerAggregator(PHASE_OVERRIDE, BooleanOrAggregator.class);
    registerAggregator(NEXT_PHASE, IntOverwriteAggregator.class);
    registerAggregator(PREPROCESSING_VERTICES_SENT, LongSumAggregator.class);
    registerAggregator(MIN_MEMORY_AVAILABLE, DoubleMinAggregator.class);

    registerPersistentAggregator(GRAPH_CLUSTERING_COEFFICIENT, DoubleSumAggregator.class);
    registerPersistentAggregator(MAX_DEGREE, LongMaxAggregator.class);
    registerPersistentAggregator(BEST_WCC, DoubleOverwriteAggregator.class);
    registerPersistentAggregator(RETRIES_LEFT, IntOverwriteAggregator.class);
    registerPersistentAggregator(PHASE, IntMinAggregator.class);
    registerPersistentAggregator(INTERPHASE_STEP, IntOverwriteAggregator.class);
    registerPersistentAggregator(NUMBER_OF_PREPROCESSING_STEPS, IntOverwriteAggregator.class);
    registerPersistentAggregator(NUMBER_OF_WCC_STEPS, IntOverwriteAggregator.class);

    // READ CUSTOM CONFIGURATION PARAMETERS
    String numPreprocessingStepsConf = getConf().get(NUMBER_OF_PREPROCESSING_STEPS_CONF_OPT);
    int numPreprocessingSteps = (numPreprocessingStepsConf!= null) ? 
        Integer.parseInt(numPreprocessingStepsConf.trim()) :
        DEFAULT_NUMBER_OF_PREPROCESSING_STEPS;
    System.out.println("Number of preprocessing steps: " + numPreprocessingSteps);
    setAggregatedValue(NUMBER_OF_PREPROCESSING_STEPS, new IntWritable(numPreprocessingSteps));

    String numWccComputationStepsConf = getConf().get(NUMBER_OF_WCC_COMPUTATION_STEPS_CONF_OPT);
    int numWccComputationSteps = (numWccComputationStepsConf != null) ? 
        Integer.parseInt(numWccComputationStepsConf.trim()) :
        DEFAULT_NUMBER_OF_WCC_COMPUTATION_STEPS;
    System.out.println("Number of WCC computation steps: " + numWccComputationSteps);
    setAggregatedValue(NUMBER_OF_WCC_STEPS, new IntWritable(numWccComputationSteps));

    String maxRetriesConf = getConf().get(MAX_RETRIES_CONF_OPT);
    maxRetries = (maxRetriesConf != null) ? 
        Integer.parseInt(maxRetriesConf.trim()) :
        DEFAULT_MAX_RETRIES;
    System.out.println("Max number of retries: " + maxRetries);
  }

  private int getPhaseAfter(int phase, boolean repeatPhase) {
    BooleanWritable phaseOverride = getAggregatedValue(PHASE_OVERRIDE);
    if (phaseOverride.get()) {
        IntWritable nextPhaseAgg = getAggregatedValue(NEXT_PHASE);
        return nextPhaseAgg.get();
    }
    if (repeatPhase)                    return phase;
    if (phase == CHOOSE_NEXT_COMMUNITY) return UPDATE_COMMUNITY_INFO;
    if (phase == COUNTING_TRIANGLES)    return SENDING_ADJACENCY_LISTS;
    return phase + 1;
  }

  private void printHeader() {
    System.out.println();
    System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
    System.out.println();
    System.out.println("WccMasterCompute: Starting superstep " + getSuperstep());
    System.out.println();
  }


  private void printMetrics() {
    System.out.println();
    System.out.println(MemoryUtils.getRuntimeMemoryStats());
    System.out.println("Master memory used: " + (MemoryUtils.totalMemoryMB() - MemoryUtils.freeMemoryMB()));

    double minWorkerMemAvailable = this.<DoubleWritable>getAggregatedValue(MIN_MEMORY_AVAILABLE).get();
    System.out.println("Minimum worker memory available: " + minWorkerMemAvailable);

    System.out.println("-----------------------------------------");
    System.out.println();
  }
}

