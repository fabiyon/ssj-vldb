package de.huberlin.mgjoin;

import de.huberlin.vernicajoin.*;
import org.apache.hadoop.util.ProgramDriver;

public class MGJoinDriver {
  public static final String TOKENSWITHFREQUENCIES_PATH_SUFFIX = "Tokenorder"; // TID Frequency (unsorted!)
  public static final String BALANCED_PATH_SUFFIX = "Balanced";
  public static final String RESULTWITHDUPLICATES_PATH_SUFFIX = VernicaJoinDriver.RID_PAIR_PATH; // "WithDuplicates";

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("tokenorder", MGTokenorderJob.class, "");
      pgd.addClass("balance", MGBalanceJob.class, "");
      pgd.addClass("join", MGJoinJob.class, "");
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
