package de.huberlin.vsmart;

import org.apache.hadoop.util.ProgramDriver;

public class VsmartJoinDriver {
  public static final String JOINPARTITIONS_SUFFIX = "JoinPartitions";

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("replicate", VsmartReplicateJob.class, "");
      pgd.addClass("similarity", VsmartSimilarityJob.class, "");
      pgd.driver(argv);
      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}
