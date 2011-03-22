package org.basex.test.data;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Random;

import org.basex.data.IdPreMap;
import org.basex.util.Performance;
import org.junit.Before;
import org.junit.Test;

/**
 * ID -> PRE map test.
 * @author Dimitar Popov
 */
public class IdPreMapTest2 {

  /** Initial number of records. */
  private static final int BASEID = 7;
  /** Random number generator. */
  private static final Random RANDOM = new Random();
  /** ID -> PRE map to compare to. */
  private DummyIdPreMap basemap;
  /** ID -> PRE map to test. */
  private IdPreMap testedmap;

  /** Set-up method. */
  @Before
  public void setUp() {
    final int[] map = new int[BASEID];
    for(int i = 0; i < map.length; i++) map[i] = i;
    basemap = new DummyIdPreMap(map);
    testedmap = new IdPreMap(map.length);
  }

  /** Test 1. */
  @Test
  public void test1() {
    int id = BASEID;

    final Performance p = new Performance();
    for(int i = 0; i < 100000; i++)
      insert(RANDOM.nextInt(++id), id);
    System.err.println("Insert time: " + p);

    check();
    System.err.println("Check time: " + p);
  }

  private void insert(final int pre, final int id) {
    basemap.insert(pre, id);
    testedmap.insert(pre, id, 1);
  }

  /** Check the two mappings. */
  private void check() {
    for(int pre = 0; pre < basemap.size(); pre++) {
      final int id = basemap.id(pre);
      assertEquals("Wrong PRE for " + id, pre, testedmap.pre(id));
    }
  }
}

/**
 * Dummy implementation of ID -> PRE map: very slow, but simple and correct.
 * @author Dimitar Popov
 */
class DummyIdPreMap {
  /** ID list. */
  final ArrayList<Integer> ids;

  /**
   * Constructor.
   * @param i initial list of ids.
   */
  public DummyIdPreMap(final int[] i) {
    ids = new ArrayList<Integer>(i.length);
    for(int k = 0; k < i.length; k++) ids.add(i[k]);
  }

  /**
   * Insert new record.
   * @param pre record PRE
   * @param id record ID
   */
  public void insert(final int pre, final int id) {
    ids.add(pre, id);
  }

  /**
   * Delete a record.
   * @param pre record PRE
   * @return deleted record ID
   */
  public int delete(final int pre) {
    return ids.remove(pre);
  }

  /**
   * PRE of the record with a given ID.
   * @param id record ID
   * @return record PRE
   */
  public int pre(final int id) {
    return ids.indexOf(id);
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
   * Size of the map.
   * @return number of stored records
   */
  public int size() {
    return ids.size();
  }
}
