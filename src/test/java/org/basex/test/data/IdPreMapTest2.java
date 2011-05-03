package org.basex.test.data;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Random;

import org.basex.data.IdPreMap;
import org.basex.util.Performance;
import org.basex.util.Util;
import org.junit.Before;
import org.junit.Test;

/**
 * ID -> PRE mapping test.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public class IdPreMapTest2 {
  /** Verbose flag. */
  private static final boolean VERBOSE = false;
  /** Number of update operations to execute in each test. */
  private static final int ITERATIONS = 7000;
  /** Initial number of records. */
  private static final int BASEID = 5000;
  /** Maximal number of bulk inserted/deleted records. */
  private static final int BULKCOUNT = 200;
  /** Random number generator. */
  private static final Random RANDOM = new Random();
  /** ID -> PRE map to compare to. */
  private DummyIdPreMap basemap;
  /** ID -> PRE map to test. */
  private IdPreMap testedmap;
  /** Sequence of inserted PRE values. */
  private int[][] inserts;
  /** Number of executed inserts. */
  private int insertcnt;
  /** Sequence of deleted PRE values. */
  private int[][] deletes;
  /** Number of executed deletes. */
  private int deletecnt;

  /** Set-up method. */
  @Before
  public void setUp() {
    final int[] map = new int[BASEID + 1];
    for(int i = 0; i < map.length; ++i) map[i] = i;
    basemap = new DummyIdPreMap(map);
    testedmap = new IdPreMap(BASEID);
    inserts = new int[ITERATIONS][3];
    insertcnt = 0;
    deletes = new int[ITERATIONS][3];
    deletecnt = 0;
  }

  /**
   * Bulk insert correctness: insert random number of values at random
   * positions.
   */
  @Test
  public void testBulkInsertCorrectness() {
    final int n = BASEID + ITERATIONS;
    int id = BASEID + 1;
    while(id <= n) {
      final int c = RANDOM.nextInt(BULKCOUNT) + 2;
      insert(RANDOM.nextInt(id), id, c);
      check();
      id += c;
    }
  }

  /** Insert correctness: insert values at random positions. */
  @Test
  public void testInsertCorrectness() {
    final int n = BASEID + ITERATIONS;
    for(int id = BASEID + 1; id <= n; ++id) {
      insert(RANDOM.nextInt(id), id, 1);
      check();
    }
  }

  /** Delete correctness: delete values at random positions. */
  @Test
  public void testDeleteCorrectness() {
    for(int id = BASEID + 1; id > 0; --id) {
      delete(RANDOM.nextInt(id));
      check();
    }
  }

  /** Delete correctness: delete values at random positions. */
  @Test
  public void testDeleteCorrectness2() {
    final int n = BASEID + ITERATIONS;
    for(int id = BASEID + 1; id <= n; ++id) insert(RANDOM.nextInt(id), id, 1);

    for(int id = n; id > 0; --id) {
      delete(RANDOM.nextInt(id));
      check();
    }
  }

  /** Correctness: randomly insert/delete value at random positions. */
  @Test
  public void testInsertDeleteCorrectness() {
    for(int i = 0, n = BASEID + 1, id = BASEID + 1; i < ITERATIONS; ++i) {
      // can't delete if all records have been deleted:
      if(RANDOM.nextBoolean() || id == 0) insert(RANDOM.nextInt(n++), id++, 1);
      else delete(RANDOM.nextInt(n--));
      check();
    }
  }

  /** Insert performance: insert at random positions. */
  @Test
  public void testBulkInsertPerformance() {
    System.err.print("Tested mapping: ");
    testBulkInsertPerformance(testedmap);
  }

  /** Insert performance: insert at random positions. */
  @Test
  public void testInsertPerformance() {
    if(VERBOSE) Util.err("Tested mapping: ");
    testInsertPerformance(testedmap);
  }

  /** Delete performance: delete at random positions. */
  @Test
  public void testDeletePerformance() {
    if(VERBOSE) Util.err("Tested mapping: ");
    testDeletePerformance(testedmap, basemap);
  }

  /** Search performance: insert at random positions and the search. */
  @Test
  public void testSearchPerformance() {
    if(VERBOSE) Util.err("Tested mapping: ");
    testSearchPerformance(testedmap);
  }

  /** Search performance: bulk insert at random positions and the search. */
  @Test
  public void testSearchBulkInsertPerformance() {
    System.err.print("Tested mapping: ");
    testSearchBulkInsertPerformance(testedmap);
  }

  /** Dummy insert performance: insert at random positions. */
  @Test
  public void testInsertPerformanceDummy() {
    if(VERBOSE) Util.err("Dummy mapping: ");
    testInsertPerformance(basemap);
  }

  /** Dummy delete performance: delete at random positions. */
  @Test
  public void testDeletePerformanceDummy() {
    if(VERBOSE) Util.err("Dummy mapping: ");
    testDeletePerformance(basemap, basemap.copy());
  }

  /** Dummy search performance: insert at random positions and the search. */
  @Test
  public void testSearchPerformanceDummy() {
    if(VERBOSE) Util.err("Dummy mapping: ");
    testSearchPerformance(basemap);
  }

  /**
   * Insert performance: insert at random positions.
   * @param m tested map
   */
  private static void testInsertPerformance(final IdPreMap m) {
    // prepare <pre, id> pairs:
    final int[][] d = new int[ITERATIONS][2];
    for(int i = 0, id = BASEID + 1; i < d.length; ++id, ++i) {
      d[i][0] = RANDOM.nextInt(id);
      d[i][1] = id;
    }

    // perform the actual test:
    final Performance p = new Performance();
    for(int i = 0; i < d.length; ++i) m.insert(d[i][0], d[i][1], 1);
    System.err.println(d.length + " records inserted in: " + p);
  }

  /**
   * Bulk insert performance: insert at random positions.
   * @param m tested map
   */
  private static void testBulkInsertPerformance(final IdPreMap m) {
    // prepare <pre, id> pairs:
    final int[][] d = new int[ITERATIONS][3];
    final int cnt = generateBulkData(d);

    // perform the actual test:
    final Performance p = new Performance();
    for(int i = 0; i < d.length; ++i) m.insert(d[i][0], d[i][1], d[i][2]);
    System.err.println(cnt + " records inserted in: " + p);
  }

  /**
   * Delete performance: delete at random positions.
   * @param m tested map
   * @param b base map
   */
  private static void testDeletePerformance(final IdPreMap m,
      final DummyIdPreMap b) {
    // prepare <pre, id> pairs:
    final int[][] d = new int[BASEID + 1][2];
    for(int i = 0, id = BASEID + 1; i < d.length; --id, ++i) {
      d[i][0] = RANDOM.nextInt(id);
      d[i][1] = b.id(d[i][0]);
      b.delete(d[i][0], d[i][1], -1);
    }

    // perform the test:
    final Performance p = new Performance();
    for(int i = 0; i < d.length; i++) m.delete(d[i][0], d[i][1], -1);
    System.err.println(d.length + " records deleted in: " + p);
  }

  /**
   * Search performance: insert at random positions and then search.
   * @param m tested map
   */
  private static void testSearchPerformance(final IdPreMap m) {
    final int n = BASEID + ITERATIONS;
    for(int id = BASEID + 1; id <= n; ++id) m.insert(RANDOM.nextInt(id), id, 1);

    final Performance p = new Performance();
    for(int i = 0; i < n; ++i) m.pre(i);
    if(VERBOSE) {
      Util.errln(n + " records found in: " + p);
      Util.errln("Mapping size: " + m.size());
    }
  }

  /**
   * Search performance: bulk insert at random positions and then search.
   * @param m tested map
   */
  private static void testSearchBulkInsertPerformance(final IdPreMap m) {
    final int[][] d = new int[ITERATIONS][3];
    int cnt = generateBulkData(d);

    for(int i = 0; i < d.length; ++i) m.insert(d[i][0], d[i][1], d[i][2]);

    final Performance p = new Performance();
    for(int i = 0; i < cnt; ++i) m.pre(i);
    System.err.println(cnt + " records found in: " + p);
    System.err.println("Mapping size: " + m.size());
  }

  /**
   * Generated bulk insert data.
   * @param d array where the test data will be stored
   * @return number of generated records (not the size of the array!)
   */
  private static int generateBulkData(final int[][] d) {
    // prepare <pre, id> pairs:
    int cnt = 0;
    for(int i = 0, id = BASEID + 1; i < d.length; ++id, ++i) {
      d[i][0] = RANDOM.nextInt(id);
      d[i][1] = id;
      d[i][2] = RANDOM.nextInt(BULKCOUNT) + 1;
      cnt += d[i][2];
    }
    return cnt;
  }

  /**
   * Insert a &lt;pre, id&gt; pair in {@link #basemap} and {@link #testedmap}.
   * @param pre pre value
   * @param id id value
   * @param c number of inserted records
   */
  private void insert(final int pre, final int id, final int c) {
    inserts[insertcnt][0] = pre;
    inserts[insertcnt][1] = id;
    inserts[insertcnt++][2] = c;
    //System.err.println("insert(" + pre + ", " + id + ")");
    testedmap.insert(pre, id, c);
    //System.err.println(testedmap);
    basemap.insert(pre, id, c);
  }

  /**
   * Delete a &lt;pre, id&gt; pair from {@link #basemap} and {@link #testedmap}.
   * @param pre pre value
   */
  private void delete(final int pre) {
    // deletedpres.add(pre);
    //if(VERBOSE) Util.errln("delete(" + pre + ", " + basemap.id(pre) + ")");
    testedmap.delete(pre, basemap.id(pre), -1);
    //if(VERBOSE) Util.errln(testedmap);
    basemap.delete(pre, basemap.id(pre), -1);
  }

  /** Check the two mappings. */
  private void check() {
    for(int pre = 0; pre < basemap.size(); pre++) {
      final int id = basemap.id(pre);
      final int p = testedmap.pre(id);
      if(pre != p) {
        final StringBuilder ins = new StringBuilder();
        for(int i = 0; i < insertcnt; i++) {
          ins.append('(');
          ins.append(inserts[i][0]); ins.append(',');
          ins.append(inserts[i][1]); ins.append(',');
          ins.append(inserts[i][2]);
          ins.append(')'); ins.append(' ');
        }
        final StringBuilder del = new StringBuilder();
        for(int i = 0; i < deletecnt; i++) {
          del.append('(');
          del.append(deletes[i][0]); del.append(',');
          del.append(deletes[i][1]); del.append(',');
          del.append(deletes[i][2]);
          del.append(')'); del.append(' ');
        }
        fail("Wrong PRE for ID = " + id + ": expected " + pre + ", actual " + p
            + "\nInserted PREs: " + ins
            + "\nDelete PREs: " + del);
      }
    }
  }
}

/**
 * Dummy implementation of ID -> PRE map: very slow, but simple and correct.
 * @author Dimitar Popov
 */
class DummyIdPreMap extends IdPreMap {
  /** ID list. */
  private final ArrayList<Integer> ids;

  /**
   * Constructor.
   * @param i initial list of ids.
   */
  public DummyIdPreMap(final int[] i) {
    super(i.length - 1);
    ids = new ArrayList<Integer>(i.length);
    for(final int element : i)
      ids.add(element);
  }

  @Override
  public void insert(final int pre, final int id, final int c) {
    for(int i = 0; i < c; i++) ids.add(pre + i, id + i);
  }

  @Override
  public void delete(final int pre, final int id, final int c) {
    for(int i = 0; i < c; i++) ids.remove(pre);
  }

  @Override
  public int pre(final int id) {
    return ids.indexOf(id);
  }

  /**
   * Size of the map.
   * @return number of stored records
   */
  @Override
  public int size() {
    return ids.size();
  }

  /**
   * ID of the record with a given PRE.
   * @param pre record PRE
   * @return record ID
   */
  public int id(final int pre) {
    return ids.get(pre);
  }

  /**
   * Create a copy of the current object.
   * @return deep copy of the object
   */
  public DummyIdPreMap copy() {
    final int[] a = new int[ids.size()];
    for(int i = size() - 1; i >= 0; --i) a[i] = ids.get(i).intValue();
    return new DummyIdPreMap(a);
  }
}
