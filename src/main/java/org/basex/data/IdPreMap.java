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
  private final IntListExt presold;
  /** Inserted ID values. */
  private final IntListExt nidsold;
  /** Increments showing how the PRE values have been modified. */
  private final IntListExt incsold;
  /** ID values for the PRE, before inserting/deleting a record. */
  private final IntListExt oidsold;
  /** Delete IDs. */
  private final BitArray deletedids;

  private int[] pres;
  private int[] fids;
  private int[] nids;
  private int[] incs;
  private int[] oids;

  private int[][] idix;

  private int rows;

  /**
   * Constructor.
   * @param id last inserted ID
   */
  public IdPreMap(final int id) {
    baseid = id;

    presold = new IntListExt(5);
    nidsold = new IntListExt(5);
    incsold = new IntListExt(5);
    oidsold = new IntListExt(5);


    rows = 0;
    pres = new int[1];
    fids = new int[pres.length];
    nids = new int[pres.length];
    incs = new int[pres.length];
    oids = new int[pres.length];

    idix = new int[pres.length][];

    deletedids = new BitArray();
  }

  /**
   * Find the PRE value of a given ID.
   * @param id ID
   * @return PRE or -1 if the I
   */
  public int pre(final int id) {
    // no updates or id is not affected by updates:
    if(rows == 0 || id < pres[0]) return id;
    // the id was deleted:
    // if(deletedids.get(id)) return -1;
    // id was inserted by update:
    if(id > baseid) {
      int i = sortedIndexOf(idix, 0, id);
      if(i < 0) {
        i = -i - 2;
        i = idix[i][1];
        return pres[i] + id - fids[i];
      }
      return pres[idix[i][1]];
    }
    // id is affected by updates:
    final int i = sortedLastIndexOf(oids, id);
    return id + incs[i < 0 ? -i - 2 : i];
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
  public void insert(final int pre, final int id, final int c) {
    int i = 0;
    int inc = c;
    int oid = pre;

    if(rows > 0) {
      i = sortedIndexOf(pres, pre);
      if(i < 0) {
        i = -i - 1;
        if(i != 0) {
          // check if inserting into an existing id interval:
          final int prevcnt = nids[i - 1] - fids[i - 1] + 1;
          final int prevpre = pres[i - 1];
          if(pre < prevpre + prevcnt) {
            // split the id interval:
            final int s = pre - prevpre;
            add(i, pre, fids[i - 1] + s, nids[i - 1], incs[i - 1], oids[i - 1],
                false);
            // decrement the number of ids:
            nids[i - 1] = fids[i - 1] + s - 1;
            // decrement the correcting value:
            incs[i - 1] += s - prevcnt;
          }
          oid = pre - incs[i - 1];
          inc += incs[i - 1];
        }
      } else if(i > 0) {
        oid = oids[i];
        inc += incs[i - 1];
      }
      add(i, pre, id, id + c - 1, inc, oid, true);
      for(int k = i + 1; k < rows; ++k) {
        pres[k] += c;
        incs[k] += c;
      }
    } else {
      add(i, pre, id, id + c - 1, inc, oid, true);
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

    if(presold.size() > 0) {
      i = presold.sortedIndexOf(pre);
      if(i < 0) {
        i = -i - 1;
        // the next entry will be the same after the correction:
        if(i < presold.size() && presold.get(i) + c == pre) {
          // re-use the next record:
          presold.inc(i, c);
          incsold.inc(i, c);
        } else {
          if(i != 0) {
            oid = pre - incsold.get(i - 1);
            inc += incsold.get(i - 1);
          }
          presold.add(pre, i);
          incsold.add(inc, i);
          nidsold.add(-1, i);
          oidsold.add(oid, i);
        }
      } else {
        // the next entry will be the same after the correction:
        if(i + 1 < presold.size() && presold.get(i + 1) + c == pre) {
          presold.remove(i);
          incsold.remove(i);
          nidsold.remove(i);
          oidsold.remove(i);

          presold.inc(i, c);
        }
        incsold.inc(i, c);
      }
      // apply the correction to all subsequent records:
      for(int k = presold.size() - 1; k > i; --k) {
        presold.inc(k, c);
        incsold.inc(k, c);
      }
    } else {
      presold.set(pre, i);
      incsold.set(inc, i);
      nidsold.set(-1, i);
      oidsold.set(oid, i);
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder();

    b.append("pres, fids, nids, incs, oids");
    for(int i = 0; i < rows; i++) {
      b.append('\n');
      b.append(pres[i]); b.append(", ");
      b.append(fids[i]); b.append(", ");
      b.append(nids[i]); b.append(", ");
      b.append(incs[i]); b.append(", ");
      b.append(oids[i]);
    }

    return b.toString();
  }

  /**
   * Size of the mapping table (only for debugging purposes!).
   * @return number of rows in the table
   */
  public int size() {
    return rows;
  }

  //
  /**
   * Searches the specified element via binary search. Note that all elements
   * must be sorted.
   * @param a array to search into
   * @param e element to be found
   * @return index of the search key, or the negative insertion point - 1
   */
  private int sortedIndexOf(final int[] a, final int e) {
    return Arrays.binarySearch(a, 0, rows, e);
  }

  /**
   * Searches the specified element via binary search. Note that all elements
   * must be sorted.
   * @param a array to search into
   * @param c column
   * @param e element to be found
   * @return index of the search key, or the negative insertion point - 1
   */
  private int sortedIndexOf(final int[][] a, final int c, final int e) {
    int low = 0;
    int high = rows - 1;
    while(low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a[mid][c];
      if(midVal < e) low = mid + 1;
      else if(midVal > e) high = mid - 1;
      else return mid; // key found
    }
    return -(low + 1); // key not found.
  }

  /**
   * Binary search for a key in a list. If there are several hits the last one
   * is returned.
   * @param a array to search into
   * @param e key to search for
   * @return index of the found hit or where the key ought to be inserted
   */
  private int sortedLastIndexOf(final int[] a, final int e) {
    int i = Arrays.binarySearch(a, 0, rows, e);
    if(i >= 0) {
      while(++i < rows && a[i] == e);
      return i - 1;
    }
    return i;
  }

  private void set(final int i, final int pre, final int fid, final int nid,
      final int inc, final int oid) {
    if(i >= pres.length) {
      final int s = Array.newSize(i + 1);
      pres = Arrays.copyOf(pres, s);
      fids = Arrays.copyOf(fids, s);
      nids = Arrays.copyOf(nids, s);
      incs = Arrays.copyOf(incs, s);
      oids = Arrays.copyOf(oids, s);
      idix = Arrays.copyOf(idix, s);
    }
    pres[i] = pre;
    fids[i] = fid;
    nids[i] = nid;
    incs[i] = inc;
    oids[i] = oid;

    idix[rows] = new int[] { fid, i};
    if(i >= rows) rows += i + 1;
  }

  private void add(final int i, final int pre, final int fid, final int nid,
      final int inc, final int oid, final boolean sid) {
    if(rows == pres.length) {
      final int s = Array.newSize(rows);
      pres = Arrays.copyOf(pres, s);
      fids = Arrays.copyOf(fids, s);
      nids = Arrays.copyOf(nids, s);
      incs = Arrays.copyOf(incs, s);
      oids = Arrays.copyOf(oids, s);
      idix = Arrays.copyOf(idix, s);
    }
    if(i < rows) {
      System.arraycopy(pres, i, pres, i + 1, rows - i);
      System.arraycopy(fids, i, fids, i + 1, rows - i);
      System.arraycopy(nids, i, nids, i + 1, rows - i);
      System.arraycopy(incs, i, incs, i + 1, rows - i);
      System.arraycopy(oids, i, oids, i + 1, rows - i);
    }
    pres[i] = pre;
    fids[i] = fid;
    nids[i] = nid;
    incs[i] = inc;
    oids[i] = oid;

    for(int j = 0; j < rows; ++j) if(idix[j][1] >= i) ++idix[j][1];
    if(sid) {
      idix[rows++] = new int[] { fid, i};
    } else {
      int k = sortedIndexOf(idix, 0, fid);
      if(k < 0) {
        k = -k - 1;
        System.arraycopy(idix, k, idix, k + 1, rows - k);
        idix[k] = new int[] { fid, i};
        rows++;
      }
    }
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
