package org.basex.data;

import org.basex.util.IntList;

/** ID -> PRE mapping. */
public class IdPreMap {
  /** Base ID value. */
  private final int baseid;
  /** PRE values of the inserted/deleted IDs. */
  private final IntList pres;
  /** Inserted ID values. */
  private final IntList ids;
  /** Increments showing how the PRE values have been modified. */
  private final IntList incs;
  /** ID values for the PRE, before inserting/deleting a record. */
  private final IntList origids;
  /** ID index for fast search of PRE values. */
  private final IntList idindex;

  /**
   * Constructor.
   * @param id last inserted ID
   */
  public IdPreMap(final int id) {
    baseid = id;

    pres = new IntList(5);
    ids = new IntList(5);
    incs = new IntList(5);
    origids = new IntList(5);

    idindex = new IntList(5);
  }

  /**
   * Find the PRE value of a given ID.
   * @param id ID
   * @return PRE
   */
  public int pre(final int id) {
    if(id < pres.get(0)) return id;
    if(id > baseid) return idindex.get(id - baseid - 1);
    return id + incs.get(searchlast(origids, id));
  }

  /**
   * Insert new record.
   * @param pre record PRE
   * @param id record ID
   * @param c number of inserted records
   */
  public void insert(final int pre, final int id, final int c) {

    int i = 0;
    int inc = 1;
    int origid = pre;

    if(pres.size() > 0) {
      i = search(pres, pre);
      if(i < 0) {
        i = -i;
        origid = pre - incs.get(i - 1);
        inc = incs.get(i - 1) + c;
      } else if(i > 0) {
        origid = origids.get(i);
        inc = incs.get(i - 1) + c;
      }
      for(int k = pres.size(); k > i; --k) {
        pres.set(pres.get(k - 1) + c, k);
        incs.set(incs.get(k - 1) + c, k);
        ids.set(ids.get(k - 1), k);
        origids.set(origids.get(k - 1), k);
      }
    }

    pres.set(pre, i);
    ids.set(id, i);
    incs.set(inc, i);
    origids.set(origid, i);

    // [DP]
    for(int k = idindex.size() - 1; k >= 0; --k) {
      final int p = idindex.get(k);
      if(p >= pre) idindex.set(p + c, k);
    }
    idindex.add(pre);
  }

  /**
   * Delete a record.
   * @param pre record PRE
   * @param c number of deleted records
   * @return deleted record ID
   */
  public int delete(final int pre, final int c) {
    return -1;
  }

  /**
   * Binary search for a key in a list. If there are several hits the last one
   * is returned.
   * @param list sorted list
   * @param key key to search for
   * @return index of the found hit or where the key ought to be inserted
   */
  private static int searchlast(final IntList list, final int key) {
    int i = search(list, key);
    if(i < 0) return -i - 1;
    while(++i < list.size() && list.get(i) == key);
    return i - 1;
  }

  /**
   * Perform binary search for a key in a list.
   * @param list sorted list
   * @param key key to search for
   * @return <li>positive index in the list where the key is found
   *         <li>negative index in the list where the key ought to be inserted
   */
  private static int search(final IntList list, final int key) {
    int low = 0;
    int high = list.size() - 1;
    while(low <= high) {
      int mid = (low + high) >>> 1;
      int val = list.get(mid);
      if(val < key) low = mid + 1;
      else if(val > key) high = mid - 1;
      else return mid;
    }
    return -low;
  }
}
