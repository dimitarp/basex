package org.basex.data;

import java.util.Arrays;
import org.basex.util.IntList;

/**
 * ID -> PRE mapping.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 * [DP] support deleted IDs
 */
public class IdPreMap {
  /** Base ID value. */
  private final int baseid;
  /** PRE values of the inserted/deleted IDs. */
  private final IntListExt pres;
  /** Inserted ID values. */
  private final IntListExt ids;
  /** Increments showing how the PRE values have been modified. */
  private final IntListExt incs;
  /** ID values for the PRE, before inserting/deleting a record. */
  private final IntListExt oids;
  /** ID index for fast search of PRE values. */
  private final IntListExt idindex;

  /**
   * Constructor.
   * @param id last inserted ID
   */
  public IdPreMap(final int id) {
    baseid = id;

    pres = new IntListExt(5);
    ids = new IntListExt(5);
    incs = new IntListExt(5);
    oids = new IntListExt(5);

    idindex = new IntListExt(5);
  }

  /**
   * Find the PRE value of a given ID.
   * @param id ID
   * @return PRE
   */
  public int pre(final int id) {
    if(id < pres.get(0)) return id;
    if(id > baseid) return idindex.get(id - baseid - 1);
    return id + incs.get(searchlast(oids, id));
  }

  /**
   * Insert new record.
   * @param pre record PRE
   * @param id record ID
   * @param c number of inserted records
   */
  public void insert(final int pre, final int id, final int c) {
    int i = 0;
    int inc = c;
    int oid = pre;

    if(pres.size() > 0) {
      i = pres.binarySearch(pre);
      if(i < 0) {
        i = -i;
        oid = pre - incs.get(i - 1);
        inc += incs.get(i - 1);
      } else if(i > 0) {
        oid = oids.get(i);
        inc += incs.get(i - 1);
      }
      // [DP] shifting can be implemented in IntList?
      for(int k = pres.size(); k > i; --k) {
        pres.set(pres.get(k - 1) + c, k);
        incs.set(incs.get(k - 1) + c, k);
      }
    } else {
      pres.set(pre, i);
      incs.set(inc, i);
      ids.set(id, i);
      oids.set(oid, i);
    }

    pres.set(pre, i);
    incs.set(inc, i);
    ids.add(id, i);
    oids.add(oid, i);

    // [DP] correction can be optimized?
    for(int k = idindex.size() - 1; k >= 0; --k) {
      final int p = idindex.get(k);
      if(p >= pre) idindex.set(p + c, k);
    }
    idindex.add(pre);
  }

  /**
   * Delete a record.
   * @param pre record PRE
   * @param id deleted record ID
   * @param c number of deleted records
   */
  public void delete(final int pre, final int id, final int c) {
    int i = 0;
    int inc = c;
    int oid = pre;

    if(pres.size() > 0) {
      i = pres.binarySearch(pre);
      if(i < 0) {
        i = -i;
        oid = pre - incs.get(i - 1);
        inc += incs.get(i - 1);
      } else {
        while(i < pres.size() && pres.get(i) == pre) i++;
        if(i > 0) {
          oid = oids.get(i - 1);
          inc += incs.get(i - 1);
        }
      }
      // [DP] shifting can be implemented in IntList?
      for(int k = pres.size(); k > i; --k) {
        pres.set(pres.get(k - 1) + c, k);
        incs.set(incs.get(k - 1) + c, k);
      }
    }

    pres.set(pre, i);
    incs.set(inc, i);
    ids.add(id, i);
    oids.add(oid, i);

    // [DP] correction can be optimized?
    // for(int k = idindex.size() - 1; k >= 0; --k) {
    // final int p = idindex.get(k);
    // if(p >= pre) idindex.set(p + c, k);
    // }
    // idindex.add(pre);
  }

  /**
   * Binary search for a key in a list. If there are several hits the last one
   * is returned.
   * @param e key to search for
   * @return index of the found hit or where the key ought to be inserted
   */
  private static int searchlast(final IntListExt list, final int key) {
    int i = list.binarySearch(key);
    if(i < 0) return -i - 1;
    while(++i < list.size() && list.get(i) == key);
    return i - 1;
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();

    b.append("pres, ids, incs, oids");
    for(int i = 0; i < pres.size(); i++) {
      b.append('\n');
      b.append(pres.get(i));
      b.append(", ");
      b.append(ids.get(i));
      b.append(", ");
      b.append(incs.get(i));
      b.append(", ");
      b.append(oids.get(i));
    }

    return b.toString();
  }
}

/**
 * This is a simple container for int values.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
class IntListExt extends IntList {

  /**
   * Constructor, specifying an initial array capacity.
   * @param c array capacity
   */
  public IntListExt(final int c) {
    super(c);
  }

  /**
   * Insert an element at the specified index position.
   * @param e element to insert
   * @param i index where to insert the element
   */
  public void add(final int e, final int i) {
    if(size == list.length) list = Arrays.copyOf(list, newSize());
    if(i < size) System.arraycopy(list, i, list, i + 1, size - i);
    ++size;
    list[i] = e;
  }

  /**
   * Remove an element from the specified index position.
   * @param i index from where to remove the element
   * @return the removed element
   */
  public int remove(final int i) {
    final int e = list[i];
    if(i < --size) System.arraycopy(list, i + 1, list, i, size - i);
    return e;
  }

  /**
   * Perform binary search for a key in the list.
   * @param e key to search for
   * @return <li>positive index in the list where the key is found <li>negative
   *         index in the list where the key ought to be inserted
   */
  public int binarySearch(final int e) {
    int low = 0;
    int high = size - 1;
    while(low <= high) {
      int mid = (low + high) >>> 1;
      int val = list[mid];
      if(val < e) low = mid + 1;
      else if(val > e) high = mid - 1;
      else return mid;
    }
    return -low;
  }
}
