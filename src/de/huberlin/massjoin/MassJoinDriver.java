package de.huberlin.massjoin;

import de.huberlin.vernicajoin.*;
import org.apache.hadoop.util.ProgramDriver;

public class MassJoinDriver {
  public static final String CANDIDATE_PATH = "MassJoinCandidates";
  public static final String SJOINOUT_PATH = "MassJoinSJoinOut";

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("bto", BTOJob.class, "");
      pgd.addClass("s1", Step1SignatureCreationJob.class, "");
      pgd.addClass("s2", Step2SJoinJob.class, "");
      pgd.addClass("s3", Step3RJoinJob.class, "");
      
      pgd.addClass("s1rs", Step1SignatureCreationRSJob.class, "");
      pgd.addClass("s2rs", Step2SJoinRSJob.class, "");
      pgd.driver(argv);
      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}
