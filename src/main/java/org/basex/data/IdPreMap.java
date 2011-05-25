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
 */
public class IdPreMap {
  /** Base ID value. */
  private int baseid;
  /** PRE values of the inserted/deleted IDs. */
  private int[] pres;
  /** Inserted first ID values. */
  private int[] fids;
  /** Inserted last ID values. */
  private int[] nids;
  /** Increments showing how the PRE values have been modified. */
  private int[] incs;
  /** ID values for the PRE, before inserting/deleting a record. */
  private int[] oids;

  /** Deleted IDs. */
  private final BitArray delids;

  /** Number of records in the table. */
  private int rows;

  /**
   * ID index; first column contains IDs (sorted); the second pointer to
   * {@link #pres}.
   */
  private int[][] idix;

  /** Number of records in the index. */
  private int ixrows;

  /**
   * Constructor.
   * @param id last inserted ID
   */
  public IdPreMap(final int id) {
    baseid = id;
    rows = ixrows = 0;
    pres = new int[1];
    fids = new int[pres.length];
    nids = new int[pres.length];
    incs = new int[pres.length];
    oids = new int[pres.length];

    delids = new BitArray();

    idix = new int[pres.length][];
  }

  /**
   * Find the PRE value of a given ID.
   * @param id ID
   * @return PRE or -1 if the ID is already deleted
   */
  public int pre(final int id) {
    // no updates or id is not affected by updates:
    if(rows == 0 || id < pres[0]) return id;
    // record was deleted:
    if(delids.get(id)) return -1;
    // id was inserted by update:
    if(id > baseid) {
      // int i = sortedIndexOf(idix, 0, id);
      // if(i < 0) {
      // i = -i - 2;
      // i = idix[i][1];
      // return pres[i] + id - fids[i];
      // }
      // return pres[idix[i][1]];
      for(int i = 0; i < rows; ++i) {
        if(fids[i] == id) return pres[i];
        if(fids[i] < id && id <= nids[i]) return pres[i] + id - fids[i];
      }
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
    if(rows == 0 && pre == id && id == baseid + 1) {
      baseid += c;
      return;
    }

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
            final int fid = fids[i - 1] + s;
            add(i, pre, fid, nids[i - 1], incs[i - 1], oids[i - 1]);
            addid(i, fid, false);
            // decrement the number of ids:
            nids[i - 1] = fids[i - 1] + s - 1;
            // decrement the correcting value:
            incs[i - 1] += s - prevcnt;
            oid = oids[i - 1];
            inc += incs[i - 1];
          } else {
            oid = pre - incs[i - 1];
            inc += incs[i - 1];
          }
        }
      } else if(i > 0) {
        oid = oids[i];
        inc += incs[i - 1];
      }
      add(i, pre, id, id + c - 1, inc, oid);
      addid(i, id, true);
      for(int k = i + 1; k < rows; ++k) {
        pres[k] += c;
        incs[k] += c;
      }
    } else {
      add(i, pre, id, id + c - 1, inc, oid);
      addid(i, id, true);
    }
  }

  /**
   * Delete a record.
   * @param pre PRE value of the record
   * @param id ID of the record
   */
  public void delete(final int pre, final int id) {
    delete(pre, id, -1);
  }

  /**
   * Delete records.
   * @param pre PRE of the first record
   * @param ids IDs of the deleted records
   * @param c number of deleted records
   */
  public void delete(final int pre, final int[] ids, final int c) {
    if(ids.length == 0) return;
    // store the deleted ids:
    for(int i = 1; i < ids.length; ++i) delids.set(ids[i]);
    delete(pre, ids[0], c);
  }

  /**
   * Delete records.
   * @param pre PRE of the first record
   * @param id ID of the first deleted record
   * @param c number of deleted records
   */
  public void delete(final int pre, final int id, final int c) {
    delids.set(id);

    int oid = id;
    // if nothing has been modified and we delete from the end, nothing to do:
    if(rows == 0 && pre == oid && oid == baseid) {
      baseid += c;
      return;
    }

    int i = 0;
    int inc = c;

    if(rows > 0) {
      final int pre1 = pre;
      final int pre2 = pre - c - 1;

      int i1 = findPre(pre1);
      int i2 = -c > 1 ? findPre(pre2) : i1;

      final boolean found1 = i1 >= 0;
      final boolean found2 = i2 >= 0;

      if(!found1) i1 = -i1 - 1;
      if(!found2) i2 = -i2 - 1;

      if(i1 >= rows) {
        add(i1, pre, -1, -1, incs[i1 - 1] + inc, oid);
        return;
      }

      final int min1 = pres[i1];
      final int max1 = pres[i1] + nids[i1] - fids[i1];

      final int min2;
      final int max2;
      if(i2 >= rows) {
        min2 = max2 = pre2 + 1;
      } else {
        min2 = pres[i2];
        max2 = pres[i2] + nids[i2] - fids[i2];
      }

      // 1. apply the correction to all subsequent records:
      for(int k = found2 ? i2 + 1 : i2; k < rows; ++k) {
        pres[k] += c;
        incs[k] += c;
      }

      if(i1 == i2) {
        // pre1 <= pre2 <= max2
        if(pre1 <= min1) {
          if(max2 < pre2) {
            // TODO
            throw new RuntimeException("i1==i2 && pre1<=min1 && max2<pre2");
          } else if(pre2 == max2) {
            if(i2 + 1 < rows && pres[i2 + 1] == pre) {
              remove(i2, i2);
            } else {
              pres[i2] = pre;
              incs[i2] += c;
              fids[i2] = -1;
              nids[i2] = -1;
            }
          } else if(min2 <= pre2 && pre2 < max2) {
            final int s2 = pre2 - min2 + 1;
            fids[i2] += s2;
            incs[i2] += c;
            pres[i1] = pre;
          } else if(pre2 < min2 - 1) {
            // add a new entry at i1
            add(i1, pre, -1, -1, i1 > 0 ? incs[i1 - 1] + c : c, oid);
          }
        } else if(min1 < pre1 && pre1 <= max1) {
          if(max2 < pre2) {
            // TODO
            throw new RuntimeException(
                "i1==i2 && min1<pre1 && pre1<=max1 && max2<pre2");
          } else if(pre2 == max2) {
            final int s1 = max1 - pre1 + 1;
            nids[i1] -= s1;
            incs[i1] -= s1;
          } else if(min2 < pre2 && pre2 < max2) {
            // split the interval
            final int s2 = pre2 - min2 + 1;
            final int fid = fids[i2] + s2;
            add(i2 + 1, pre2 + c + 1, fid, nids[i2], incs[i1] + c, oids[i2]);
            addid(i2 + 1, fid, false);

            final int s1 = max1 - pre1 + 1;
            nids[i1] -= s1;
            incs[i1] -= s1;
          }
        } else if(max1 < pre1) {
          // TODO
          throw new RuntimeException("i1 == i2 && max1 < pre1");
        }
      } else if(i1 < i2) {
        // pre1 <= max1 < pre2 <= max2
        // min1 <= max1 < min2 <= max2
        if(pre1 <= min1) {
          if(pre2 == max2) {
            // remove i2
            if(i2 + 1 < rows && pres[i2 + 1] == pre) {
              remove(i1, i2);
            } else {
              incs[i2] += c;
              pres[i2] = pre;
              fids[i2] = -1;
              nids[i2] = -1;
              remove(i1, i2 - 1);
            }
          } else if(min2 <= pre2 && pre2 < max2) {
            // update the entry correspondingly
            final int s2 = pre2 - min2 + 1;
            fids[i2] += s2;
            incs[i2] += c;
            pres[i2] = pre;

            remove(i1, i2 - 1);
          } else if(pre2 < min2) {
            --i2;
            if(i2 + 1 < rows && pres[i2 + 1] == pre) {
              remove(i1, i2);
            } else {
              incs[i2] += c;
              pres[i2] = pre;
              fids[i2] = -1;
              nids[i2] = -1;
              remove(i1, i2 - 1);
            }
          }
        } else if(min1 < pre1) {
          if(pre2 == max2) {
            inc += incs[i2];
            oid = oids[i2];

            remove(i1 + 1, i2);

            final int s1 = max1 - pre1 + 1;
            nids[i1] -= s1;
            incs[i1] = inc;
            oids[i1] = oid;
          } else if(min2 <= pre2 && pre2 < max2) {
            // update the entry correspondingly
            final int s2 = pre2 - min2 + 1;
            fids[i2] += s2;
            incs[i2] += c;
            pres[i2] = pre;

            remove(i1 + 1, i2 - 1);

            final int s1 = max1 - pre1 + 1;
            nids[i1] -= s1;
            incs[i1] -= s1;
          } else if(pre2 < min2) {
            incs[i1] = incs[i2 - 1] + c;
            remove(i1 + 1, i2 - 1);

            final int s1 = max1 - pre1 + 1;
            nids[i1] -= s1;
          }
        }
      }
    } else {
      add(i, pre, -1, -1, inc, oid);
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
   * Search for a given pre value.
   * @param pre pre value
   * @return index of the record where the pre was found, or the negative
   *         insertion point - 1
   */
  private int findPre(final int pre) {
    int low = 0;
    int high = rows - 1;
    while(low <= high) {
      int mid = (low + high) >>> 1;
      final int midValMin = pres[mid];
      final int midValMax = midValMin + nids[mid] - fids[mid];
      if(midValMax < pre) low = mid + 1;
      else if(midValMin > pre) high = mid - 1;
      else return mid; // key found
    }
    return -(low + 1); // key not found.
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
    int high = ixrows - 1;
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

  /**
   * Add a record to the table and the ID index.
   * @param i index in the table where the record should be inserted
   * @param pre pre value
   * @param fid first ID value
   * @param nid last ID value
   * @param inc increment value
   * @param oid original ID value
   */
  private void add(final int i, final int pre, final int fid, final int nid,
      final int inc, final int oid) {
    if(rows == pres.length) {
      final int s = Array.newSize(rows);
      pres = Arrays.copyOf(pres, s);
      fids = Arrays.copyOf(fids, s);
      nids = Arrays.copyOf(nids, s);
      incs = Arrays.copyOf(incs, s);
      oids = Arrays.copyOf(oids, s);
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
    ++rows;
    for(int j = 0; j < ixrows; ++j) if(idix[j][1] >= i) ++idix[j][1];
  }

  /**
   * Add a record to the ID index.
   * @param i index in the table where the record should be inserted
   * @param fid first ID value
   * @param sid is the ID newly generated (i.e. bigger than any other)
   */
  private void addid(final int i, final int fid, final boolean sid) {
    if(ixrows == idix.length) {
      idix = Arrays.copyOf(idix, Array.newSize(ixrows));
    }
    if(sid) {
      idix[ixrows] = new int[] { fid, i};
    } else {
      final int k = -sortedIndexOf(idix, 0, fid) - 1;
      if(k < ixrows) {
        System.arraycopy(idix, k, idix, k + 1, ixrows - k);
      }
      idix[k] = new int[] { fid, i};
    }
    ++ixrows;
  }

  /**
   * Remove a records from the table and the ID index.
   * @param s start index of records in the table (inclusive)
   * @param e end index of records in the table (inclusive)
   */
  private void remove(final int s, final int e) {
    if(s <= e) {
      for(int j = s; j <= e; ++j) if(fids[j] >= 0) removeid(fids[j]);
      final int c = s - e - 1;
      for(int j = 0; j < ixrows; ++j) if(idix[j][1] >= s) idix[j][1] += c;

      System.arraycopy(pres, e + 1, pres, s, rows - (e + 1));
      System.arraycopy(fids, e + 1, fids, s, rows - (e + 1));
      System.arraycopy(nids, e + 1, nids, s, rows - (e + 1));
      System.arraycopy(incs, e + 1, incs, s, rows - (e + 1));
      System.arraycopy(oids, e + 1, oids, s, rows - (e + 1));
      rows -= e - s + 1;
    }
  }

  /**
   * Remove a record from the ID index.
   * @param id ID value to remove
   */
  private void removeid(final int id) {
    final int k = sortedIndexOf(idix, 0, id);
    if(k >= 0 && k + 1 < ixrows) {
      System.arraycopy(idix, k + 1, idix, k, ixrows - (k + 1));
    }
    --ixrows;
  }
}
