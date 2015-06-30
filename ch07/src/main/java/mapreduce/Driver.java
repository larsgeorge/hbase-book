package mapreduce;

import org.apache.hadoop.util.ProgramDriver;

/**
 * Offers choices for included MapReduce jobs.
 */
public class Driver {

  /**
   * Main entry point for jar file.
   *
   * @param args  The command line parameters.
   * @throws Throwable When the selection fails.
   */
  public static void main(String[] args) throws Throwable {
    ProgramDriver pgd = new ProgramDriver();
    pgd.addClass(ImportFromFile.NAME, ImportFromFile.class,
      "Import from file");
    pgd.addClass(ImportFromFile2.NAME, ImportFromFile2.class,
      "Import from file (with dependencies)");
    pgd.addClass(AnalyzeData.NAME, AnalyzeData.class,
      "Analyze imported JSON");
    pgd.addClass(AnalyzeSnapshotData.NAME, AnalyzeSnapshotData.class,
      "Analyze imported JSON from snapshot");
    pgd.addClass(ParseJson.NAME, ParseJson.class,
      "Parse JSON into columns");
    pgd.addClass(ParseJson2.NAME, ParseJson2.class,
      "Parse JSON into columns (map only)");
    pgd.addClass(ParseJsonMulti.NAME, ParseJsonMulti.class,
      "Parse JSON into multiple tables");
    pgd.driver(args);
  }
}
