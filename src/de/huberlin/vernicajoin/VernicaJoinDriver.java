package de.huberlin.vernicajoin;

import org.apache.hadoop.util.ProgramDriver;

public class VernicaJoinDriver {
  public static final String TOKENORDER_PATH_SUFFIX = "Tokenorder";
  public static final String RID_PAIR_PATH = "RidPairs";

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("bto", BTOJob.class, "");
      pgd.addClass("pk", VernicaJob.class, "");
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
