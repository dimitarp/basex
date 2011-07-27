package org.basex.test.data;

import java.util.Random;

import org.basex.index.IdPreMap;
import org.basex.test.data.IdPreMapBulkTestBase.DummyIdPreMap;
import org.basex.util.Performance;
import org.basex.util.Util;
import org.junit.Before;
import org.junit.Test;

/**
 * ID -> PRE mapping performance tests.
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class IdPreMapPerfTest {
  /** Verbose flag. */
  private static final boolean VERBOSE = false;
  /** Random number generator. */
  private static final Random RANDOM = new Random();
  /** Number of update operations to execute in each test. */
  protected int opcount = 7000;
  /** Initial number of records. */
  protected int baseid = 400;
  /** Maximal number of bulk inserted/deleted records. */
  protected int bulkcount = 100;
  /** ID -> PRE map to compare to. */
  protected DummyIdPreMap basemap;
  /** ID -> PRE map to test. */
  protected IdPreMap testedmap;

  /** Set-up method. */
  @Before
  public void setUp() {
    final int[] map = new int[baseid + 1];
    for(int i = 0; i < map.length; ++i) map[i] = i;
    basemap = new DummyIdPreMap(map);
    testedmap = new IdPreMap(baseid);
  }

  /** Insert performance: insert at random positions. */
  @Test
  public void testBulkInsertPerformance() {
    // prepare <pre, id> pairs:
    final int[][] d = new int[opcount][3];
    final int cnt = generateBulkData(d);

    // perform the actual test:
    final Performance p = new Performance();
    for(int i = 0; i < d.length; ++i)
      testedmap.insert(d[i][0], d[i][1], d[i][2]);
    if(VERBOSE)
      Util.err(cnt + " records with " + d.length + " inserts in: " + p);
  }

  /** Delete performance: delete at random positions. */
  @Test
  public void testBulkDeletePerformance() {
    // TODO
  }

  /** Insert and delete performance: insert/delete at random positions. */
  @Test
  public void testBulkInsertDeletePerformance() {
    // TODO
  }

  /** Search performance: bulk insert at random positions and the search. */
  @Test
  public void testSearchAfterBulkInsertPerformance() {
    final int[][] d = new int[opcount][3];
    int cnt = generateBulkData(d);

    for(int i = 0; i < d.length; ++i)
      testedmap.insert(d[i][0], d[i][1], d[i][2]);

    final Performance p = new Performance();
    for(int i = 0; i < cnt; ++i)
      testedmap.pre(i);
    if(VERBOSE) {
      Util.err(cnt + " records found in: " + p);
      Util.err("Mapping size: " + testedmap.size());
    }
  }

  /**
   * Generated bulk insert data.
   * @param d array where the test data will be stored
   * @return number of generated records (not the size of the array!)
   */
  private int generateBulkData(final int[][] d) {
    // prepare <pre, id> pairs:
    int cnt = 0;
    for(int i = 0, id = baseid + 1; i < d.length; id += d[i][2], ++i) {
      d[i][0] = RANDOM.nextInt(id);
      d[i][1] = id;
      d[i][2] = RANDOM.nextInt(bulkcount) + 1;
      cnt += d[i][2];
    }
    return cnt;
  }
}
