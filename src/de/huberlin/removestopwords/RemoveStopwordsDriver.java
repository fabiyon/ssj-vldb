package de.huberlin.removestopwords;

import de.huberlin.vernicajoin.*;
import org.apache.hadoop.util.ProgramDriver;

public class RemoveStopwordsDriver {
  public static final String TOKENORDER_PATH_SUFFIX = "Tokenorder";

  public static void main(String argv[]) {
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("bto", BTOJob.class, "");
      pgd.addClass("remove", RemoveStopwordsJob.class, "");
      pgd.driver(argv);
      // Success
      exitCode = 0;
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}
