package org.basex.data;

import java.util.Arrays;

import org.basex.util.Array;
import org.basex.util.BitArray;
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
  private final IntListExt nids;
  /** Increments showing how the PRE values have been modified. */
  private final IntListExt incs;
  /** ID values for the PRE, before inserting/deleting a record. */
  private final IntListExt oids;
  /** Delete IDs. */
  private final BitArray deletedids;

  private static final int PRES = 0;
  private static final int FIDS = 1;
  private static final int NIDS = 2;
  private static final int INCS = 3;
  private static final int OIDS = 4;

  private final IntTable mapping;

  /**
   * Constructor.
   * @param id last inserted ID
   */
  public IdPreMap(final int id) {
    baseid = id;

    pres = new IntListExt(5);
    nids = new IntListExt(5);
    incs = new IntListExt(5);
    oids = new IntListExt(5);

    mapping = new IntTable(5);

    deletedids = new BitArray();
  }

  /**
   * Find the PRE value of a given ID.
   * @param id ID
   * @return PRE or -1 if the I
   */
  public int pre2(final int id) {
    // no updates or id is not affected by updates:
    if(pres.size() == 0 || id < pres.get(0)) return id;
    // the id was deleted:
    if(deletedids.get(id)) return -1;
    // id was inserted by update:
    if(id > baseid) {
      final int s = nids.size();
      for(int i = 0; i < s; ++i) {
        final int cid = nids.get(i);
        if(id == cid) return pres.get(i);
        if(id > cid) {
          final int c = i == 0 ? incs.get(i) : incs.get(i) - incs.get(i - 1);
          // the id is in the interval:
          if(id < cid + c) return pres.get(i) + id - cid;
        }
      }
      return -1;
    }
    // id is affected by updates:
    final int i = oids.sortedLastIndexOf(id);
    return id + incs.get(i < 0 ? -i - 2 : i);
  }

  /**
   * Find the PRE value of a given ID.
   * @param id ID
   * @return PRE or -1 if the I
   */
  public int pre(final int id) {
    // no updates or id is not affected by updates:
    if(mapping.rows == 0 || id < mapping.data[PRES][0]) return id;
    // the id was deleted:
    if(deletedids.get(id)) return -1;
    // id was inserted by update:
    if(id > baseid) {
      for(int i = 0; i < mapping.rows; ++i) {
        final int cid = mapping.data[FIDS][i];
        if(id == cid) return mapping.data[PRES][i];
        if(id > cid) {
          final int c = i == 0 ? mapping.data[INCS][i] : mapping.data[INCS][i]
              - mapping.data[INCS][i - 1];
          // the id is in the interval:
          if(id < cid + c)
            return mapping.data[PRES][i] + id - cid;
        }
      }
      return -1;
    }
    // id is affected by updates:
    final int i = mapping.sortedLastIndexOf(OIDS, id);
    return id + mapping.data[INCS][i < 0 ? -i - 2 : i];
  }

  /**
   * Find the PRE values of a given list of IDs.
   * @param ids IDs
   * @return a sorted array of PRE values
   */
  public int[] pre(final int[] ids) {
    return pre(ids, 0, ids.length);
  }

  /**
   * Find the PRE values of a given list of IDs.
   * @param ids IDs
   * @param off start position in ids (inclusive)
   * @param len number of ids
   * @return a sorted array of PRE values
   */
  public int[] pre(final int[] ids, final int off, final int len) {
    final IntList p = new IntList(ids.length);
    for(int i = off; i < len; ++i) {
      final int pre = pre(ids[i]);
      if(pre >= 0) p.add(pre);
    }
    return p.sort().toArray();
  }

  /**
   * Insert new record.
   * @param pre record PRE
   * @param id record ID
   * @param c number of inserted records
   */
  public void insert2(final int pre, final int id, final int c) {
    int i = 0;
    int inc = c;
    int oid = pre;

    if(pres.size() > 0) {
      i = pres.sortedIndexOf(pre);
      if(i < 0) {
        i = -i - 1;
        if(i != 0) {
          // check if inserting into an existing id interval:
          final int prevcnt = mapping.data[NIDS][i - 1];
          final int prevpre = pres.get(i - 1);
          if(pre < prevpre + prevcnt) {
            // split the id interval:
            final int s = pre - prevpre;
            pres.add(pre, i);
            nids.add(nids.get(i - 1) + s, i);
            incs.add(incs.get(i - 1), i);
            oids.add(oids.get(i - 1), i);

            incs.inc(i - 1, s - prevcnt);
          }
          oid = pre - incs.get(i - 1);
          inc += incs.get(i - 1);
        }
      } else if(i > 0) {
        oid = oids.get(i);
        inc += incs.get(i - 1);
      }
      pres.add(pre, i);
      incs.add(inc, i);
      nids.add(id, i);
      oids.add(oid, i);
      for(int k = pres.size() - 1; k > i; --k) {
        pres.inc(k, c);
        incs.inc(k, c);
      }
    } else {
      pres.set(pre, i);
      incs.set(inc, i);
      nids.set(id, i);
      oids.set(oid, i);
    }
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

    if(mapping.rows > 0) {
      i = mapping.sortedIndexOf(PRES, pre);
      if(i < 0) {
        i = -i - 1;
        if(i != 0) {
          // check if inserting into an existing id interval:
          final int prevcnt = mapping.data[NIDS][i - 1];
          final int prevpre = mapping.data[PRES][i - 1];
          if(pre < prevpre + prevcnt) {
            // split the id interval:
            final int s = pre - prevpre;
            mapping.add(i,
                pre,
                mapping.data[FIDS][i - 1] + s,
                c, // [DP] falsch!
                mapping.data[INCS][i - 1],
                mapping.data[OIDS][i - 1]);
            // decrement the number of ids:
            mapping.data[NIDS][i - 1] += s - prevcnt;
            // decrement the correcting value:
            mapping.data[INCS][i - 1] += s - prevcnt;
          }
          oid = pre - mapping.data[INCS][i - 1];
          inc += mapping.data[INCS][i - 1];
        }
      } else if(i > 0) {
        oid = mapping.data[OIDS][i];
        inc += mapping.data[INCS][i - 1];
      }
      mapping.add(i, pre, id, c, inc, oid);
      for(int k = i + 1; k < mapping.rows; ++k) {
        mapping.data[PRES][k] += c;
        mapping.data[INCS][k] += c;
      }
    } else {
      mapping.set(i, pre, id, c, inc, oid);
    }
  }

  /**
   * Delete a record.
   * @param pre record PRE
   * @param id deleted record ID
   * @param c number of deleted records
   */
  public void delete(final int pre, final int id, final int c) {
    deletedids.set(id);

    int i = 0;
    int inc = c;
    int oid = pre;

    if(pres.size() > 0) {
      i = pres.sortedIndexOf(pre);
      if(i < 0) {
        i = -i - 1;
        // the next entry will be the same after the correction:
        if(i < pres.size() && pres.get(i) + c == pre) {
          // re-use the next record:
          pres.inc(i, c);
          incs.inc(i, c);
        } else {
          if(i != 0) {
            oid = pre - incs.get(i - 1);
            inc += incs.get(i - 1);
          }
          pres.add(pre, i);
          incs.add(inc, i);
          nids.add(-1, i);
          oids.add(oid, i);
        }
      } else {
        // the next entry will be the same after the correction:
        if(i + 1 < pres.size() && pres.get(i + 1) + c == pre) {
          pres.remove(i);
          incs.remove(i);
          nids.remove(i);
          oids.remove(i);

          pres.inc(i, c);
        }
        incs.inc(i, c);
      }
      // apply the correction to all subsequent records:
      for(int k = pres.size() - 1; k > i; --k) {
        pres.inc(k, c);
        incs.inc(k, c);
      }
    } else {
      pres.set(pre, i);
      incs.set(inc, i);
      nids.set(-1, i);
      oids.set(oid, i);
    }
  }

  public String toString2() {
    final StringBuilder b = new StringBuilder();

    b.append("pres, ids, incs, oids");
    for(int i = 0; i < pres.size(); i++) {
      b.append('\n');
      b.append(pres.get(i));
      b.append(", ");
      b.append(nids.get(i));
      b.append(", ");
      b.append(incs.get(i));
      b.append(", ");
      b.append(oids.get(i));
    }

    return b.toString();
  }

  public String toString() {
    final StringBuilder b = new StringBuilder();

    b.append("pres, fids, nids, incs, oids");
    for(int i = 0; i < mapping.rows; i++) {
      b.append('\n');
      b.append(mapping.data[PRES][i]);
      b.append(", ");
      b.append(mapping.data[FIDS][i]);
      b.append(", ");
      b.append(mapping.data[NIDS][i]);
      b.append(", ");
      b.append(mapping.data[INCS][i]);
      b.append(", ");
      b.append(mapping.data[OIDS][i]);
    }

    return b.toString();
  }

  /**
   * Size of the mapping table (only for debugging purposes!).
   * @return number of rows in the table
   */
  public int size() {
    return mapping.rows;
  }
}

class IntTable {
  public final int cols;
  public int rows;
  public int[][] data;

  public IntTable(final int c) {
    cols = c;
    data = new int[cols][0];
  }

  public void set(final int i, final int... vals) {
    if(i >= data[0].length) {
      for(int c = 0; c < cols; ++c)
        data[c] = Arrays.copyOf(data[c], Array.newSize(i + 1));
      rows += i + 1;
    }
    for(int c = 0; c < cols; ++c) data[c][i] = vals[c];
  }

  public void add(final int i, final int... vals) {
    if(rows == data[0].length) {
      for(int c = 0; c < cols; ++c)
        data[c] = Arrays.copyOf(data[c], Array.newSize(rows));
    }
    if(i < rows)
      for(int c = 0; c < cols; ++c)
        System.arraycopy(data[c], i, data[c], i + 1, rows - i);
    ++rows;
    for(int c = 0; c < cols; ++c) data[c][i] = vals[c];
  }

  /**
   * Searches the specified element via binary search. Note that all elements
   * must be sorted.
   * @param c column index
   * @param e element to be found
   * @return index of the search key, or the negative insertion point - 1
   */
  public final int sortedIndexOf(final int c, final int e) {
    return Arrays.binarySearch(data[c], 0, rows, e);
  }

  /**
   * Binary search for a key in a list. If there are several hits the last one
   * is returned.
   * @param c column index
   * @param e key to search for
   * @return index of the found hit or where the key ought to be inserted
   */
  public int sortedLastIndexOf(final int c, final int e) {
    int i = Arrays.binarySearch(data[c], 0, rows, e);
    if(i >= 0) {
      while(++i < rows && data[c][i] == e);
      return i - 1;
    }
    return i;
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
   * Binary search for a key in a list. If there are several hits the last one
   * is returned.
   * @param e key to search for
   * @return index of the found hit or where the key ought to be inserted
   */
  public int sortedLastIndexOf(final int e) {
    int i = this.sortedIndexOf(e);
    if(i >= 0) {
      while(++i < size && list[i] == e);
      return i - 1;
    }
    return i;
  }

  /**
   * Search for a key in the list.
   * @param e key to search for
   * @return index of the found hit or -1 if the key is not found
   */
  public int indexOf(final int e) {
    for(int i = 0; i < size; ++i) if(list[i] == e) return i;
    return -1;
  }

  /**
   * Increment the value of the element at the specified index.
   * @param i index of the element
   * @param c increment value
   */
  public void inc(final int i, final int c) {
    list[i] += c;
  }

  /**
   * Increment the values within an interval.
   * @param from start index (inclusive)
   * @param to end index (exclusive)
   * @param c increment value
   */
  public void inc(final int from, final int to, final int c) {
    for(int i = from; i < to; ++i) list[i] += c;
  }
}
