package org.basex.test.data;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Random;

import org.basex.data.IdPreMap;
import org.basex.util.IntList;
import org.basex.util.Performance;
import org.junit.Before;
import org.junit.Test;

/**
 * ID -> PRE map test.
 * @author Dimitar Popov
 */
public class IdPreMapTest2 {
  /** Number of update operations to execute in each test. */
  private static final int ITERATIONS = 10000;
  /** Initial number of records. */
  private static final int BASEID = 70000;
  /** Random number generator. */
  private static final Random RANDOM = new Random();
  /** ID -> PRE map to compare to. */
  private DummyIdPreMap basemap;
  /** ID -> PRE map to test. */
  private IdPreMap testedmap;
  /** Sequence of inserted PRE values. */
  private IntList insertedpres;

  /** Set-up method. */
  @Before
  public void setUp() {
    final int[] map = new int[BASEID + 1];
    for(int i = 0; i < map.length; i++) map[i] = i;
    basemap = new DummyIdPreMap(map);
    testedmap = new IdPreMap(BASEID);
    insertedpres = new IntList();
  }

  /** Insert correctness: insert values at random positions. */
  @Test
  public void testInsertCorrectness() {
    for(int i = 0, id = BASEID; i < ITERATIONS; i++)
      insert(RANDOM.nextInt(++id), id);
    check();
  }

  /** Insert performance: insert at random positions. */
  @Test
  public void testInsertPerformance() {
    testInsertPerformance(testedmap);
  }

  /** Search performance: insert at random positions and the search. */
  @Test
  public void testSearchPerformance() {
    testSearchPerformance(testedmap);
  }

  /** Dummy insert performance: insert at random positions. */
  @Test
  public void testInsertPerformanceDummy() {
    testInsertPerformance(basemap);
  }

  /** Dummy search performance: insert at random positions and the search. */
  @Test
  public void testSearchPerformanceDummy() {
    testSearchPerformance(basemap);
  }

  /**
   * Insert performance: insert at random positions.
   * @param m tested map
   */
  private static void testInsertPerformance(final IdPreMap m) {
    final Performance p = new Performance();
    for(int i = 0, id = BASEID; i < ITERATIONS; i++)
      m.insert(RANDOM.nextInt(++id), id, 1);
    System.err.println(ITERATIONS + " records inserted in: " + p);
  }

  /**
   * Search performance: insert at random positions and then search.
   * @param m tested map
   */
  private static void testSearchPerformance(final IdPreMap m) {
    int id = BASEID;
    for(int i = 0; i < ITERATIONS; i++) m.insert(RANDOM.nextInt(++id), id, 1);
    final Performance p = new Performance();
    for(int i = 0; i <= id; i++) m.pre(i);
    System.err.println(id + " records found in: " + p);
  }

  /**
   * Insert a &lt;pre, id&gt; pair in {@link #basemap} and {@link #testedmap}.
   * @param pre pre value
   * @param id id value
   */
  private void insert(final int pre, final int id) {
    insertedpres.add(pre);
    basemap.insert(pre, id, 1);
    testedmap.insert(pre, id, 1);
  }

  /** Check the two mappings. */
  private void check() {
    for(int pre = 0; pre < basemap.size(); pre++) {
      final int id = basemap.id(pre);
      final int p = testedmap.pre(id);
      if(pre != p) fail("Wrong PRE for ID = " + id + ": expected " + pre
          + ", actual " + p + "\nInserted PREs: " + insertedpres);
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
    for(int k = 0; k < i.length; k++) ids.add(i[k]);
  }

  @Override
  public void insert(final int pre, final int id, final int c) {
    ids.add(pre, id);
  }

  @Override
  public int delete(final int pre, final int c) {
    return ids.remove(pre);
  }

  @Override
  public int pre(final int id) {
    return ids.indexOf(id);
  }

  /**
   * Size of the map.
   * @return number of stored records
   */
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
}
