package de.huberlin.clusterjoin;

import de.huberlin.vernicajoin.*;
import org.apache.hadoop.util.ProgramDriver;

public class ClusterJoinDriver {
  public static String CARDINALITY_SUFFIX = "Cardinality";

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("cardinality", CountCardinalityJob.class, "");
      pgd.addClass("join", ClusterJoinJob.class, "");
      pgd.addClass("dedup", DedupJob.class, "");
      pgd.driver(argv);
      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}
