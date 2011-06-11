package org.basex.index;

import static org.basex.core.Text.*;
import static org.basex.data.DataText.*;
import static org.basex.util.Token.*;
import java.io.IOException;
import org.basex.data.Data;
import org.basex.io.DataAccess;
import org.basex.util.IntList;
import org.basex.util.Num;
import org.basex.util.Performance;
import org.basex.util.TokenBuilder;
import org.basex.util.TokenList;
import org.basex.util.TokenObjMap;

/**
 * This class provides access to attribute values and text contents
 * stored on disk. The data structure is described in the {@link ValueBuilder}
 * class.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class DiskValues implements Index {
  /** Number of index entries. */
  private int size;
  /** ID references. */
  private final DataAccess idxr;
  /** ID lists. */
  private final DataAccess idxl;
  /** Value type (texts/attributes). */
  private final boolean text;
  /** Data reference. */
  private final Data data;
  /** Cache tokens. */
  private final IndexCache cache = new IndexCache();
  /** Cached texts. Increases used memory, but speeds up repeated queries. */
  private final TokenList ctext;

  /**
   * Constructor, initializing the index structure.
   * @param d data reference
   * @param txt value type (texts/attributes)
   * @throws IOException IO Exception
   */
  public DiskValues(final Data d, final boolean txt) throws IOException {
    this(d, txt, txt ? DATATXT : DATAATV);
  }

  /**
   * Constructor, initializing the index structure.
   * @param d data reference
   * @param txt value type (texts/attributes)
   * @param pref file prefix
   * @throws IOException IO Exception
   */
  DiskValues(final Data d, final boolean txt, final String pref)
      throws IOException {
    data = d;
    text = txt;
    idxl = new DataAccess(d.meta.file(pref + 'l'));
    idxr = new DataAccess(d.meta.file(pref + 'r'));
    size = idxl.read4();
    ctext = new TokenList(size);
  }

  @Override
  public byte[] info() {
    final TokenBuilder tb = new TokenBuilder();
    tb.add(INDEXSTRUC + TREESTRUC + NL);
    final long l = idxl.length() + idxr.length();
    tb.add(SIZEDISK + Performance.format(l, true) + NL);
    final IndexStats stats = new IndexStats(data);
    for(int m = 0; m < size; ++m) {
      final long pos = idxr.read5(m * 5L);
      final int oc = idxl.readNum(pos);
      if(stats.adding(oc)) stats.add(data.text(firstpre(pos), text));
    }
    stats.print(tb);
    return tb.finish();
  }

  @Override
  public IndexIterator iter(final IndexToken tok) {
    if(tok instanceof RangeToken) return idRange((RangeToken) tok);

    final int id = cache.id(tok.get());
    if(id > 0) return iter(cache.size(id), cache.pointer(id));

    final int ix = get(tok.get());
    if(ix < 0) return IndexIterator.EMPTY;
    final long pos = idxr.read5(ix * 5L);
    return iter(idxl.readNum(pos), idxl.pos());
  }

  @Override
  public int count(final IndexToken it) {
    if(it instanceof RangeToken) return idRange((RangeToken) it).size();
    if(it.get().length > MAXLEN) return Integer.MAX_VALUE;

    final byte[] tok = it.get();
    final int id = cache.id(tok);
    if(id > 0) return cache.size(id);

    final int ix = get(tok);
    if(ix < 0) return 0;
    final long pos = idxr.read5(ix * 5L);
    // the first number is the number of hits:
    final int nr = idxl.readNum(pos);
    cache.add(it.get(), nr, pos + Num.length(nr));
    return nr;
  }

  /**
   * Returns next values. Called by the {@link ValueBuilder}.
   * @return compressed values
   */
  byte[] nextValues() {
    return idxr.pos() >= idxr.length() ? EMPTY :
      idxl.readBytes(idxr.read5(), idxl.read4());
  }

  /**
   * Iterator method.
   * @param s number of values
   * @param ps offset
   * @return iterator
   */
  private IndexIterator iter(final int s, final long ps) {
    final IntList pres = new IntList(s);
    long p = ps;
    for(int l = 0, v = 0; l < s; ++l) {
      v += idxl.readNum(p);
      p = idxl.pos();
      pres.add(data.pre(v));
    }
    return iter(pres.sort());
  }

  /**
   * Performs a range query. All index values must be numeric.
   * @param tok index term
   * @return results
   */
  private IndexIterator idRange(final RangeToken tok) {
    final double min = tok.min;
    final double max = tok.max;

    // check if min and max are positive integers with the same number of digits
    final int len = max > 0 && (long) max == max ? token(max).length : 0;
    final boolean simple = len != 0 && min > 0 && (long) min == min &&
      token(min).length == len;

    final IntList pres = new IntList();
    for(int l = 0; l < size; ++l) {
      final int ds = idxl.readNum(idxr.read5(l * 5L));
      int id = idxl.readNum();
      final int pre = data.pre(id);
      final double v = data.textDbl(pre, text);

      if(v >= min && v <= max) {
        // value is in range
        for(int d = 0; d < ds; ++d) {
          pres.add(data.pre(id));
          id += idxl.readNum();
        }
      } else if(simple && v > max && data.textLen(pre, text) == len) {
        // if limits are integers, if min, max and current value have the same
        // string length, and if current value is larger than max, test can be
        // skipped, as all remaining values will be bigger
        break;
      }
    }
    return iter(pres.sort());
  }

  /**
   * Returns an iterator for the specified id list.
   * @param ids id list
   * @return iterator
   */
  private IndexIterator iter(final IntList ids) {
    return new IndexIterator() {
      int p = -1;

      @Override
      public boolean more() { return ++p < ids.size(); }
      @Override
      public int next() { return ids.get(p); }
      @Override
      public double score() { return -1; }
    };
  }

  /**
   * Get the pre value of the first non-deleted id from the id-list at the
   * specified position.
   * @param pos position of the id-list in {@link #idxl}
   * @return {@code -1} if the id-list contains only deleted ids
   */
  private int firstpre(final long pos) {
    // read the number of ids in the list
    idxl.readNum(pos);
    return data.pre(idxl.readNum());
  }

  /**
   * Binary search for key in the {@link #idxr}.
   * @param key token to be found
   * @return if the key is found: index of the key else: -(insertion point - 1)
   */
  private int get(final byte[] key) {
    return get(key, 0, size - 1);
  }

  /**
   * Binary search for key in the {@link #idxr}.
   * @param key token to be found
   * @param first begin of the search interval
   * @param last end of the search interval
   * @return if the key is found: index of the key else: -(insertion point - 1)
   */
  private int get(final byte[] key, final int first, final int last) {
    int l = first, h = last;
    while(l <= h) {
      final int m = l + h >>> 1;
      byte[] txt = ctext.get(m);
      if(txt == null) {
        txt = data.text(firstpre(idxr.read5(m * 5L)), text);
        ctext.set(txt, m);
      }
      final int d = diff(txt, key);
      if(d == 0) return m;
      if(d < 0) l = m + 1;
      else h = m - 1;
    }
    return -(l + 1);
  }

  /**
   * Flushes the buffered data.
   * @throws IOException I/O exception
   */
  public void flush() throws IOException {
    idxl.flush();
    idxr.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    idxl.close();
    idxr.close();
  }

  /**
   * Add entries to the index.
   * @param m a set of <key, id-list> pairs
   */
  public void index(final TokenObjMap<IntList> m) {
    // create a sorted list of all keys
    final TokenList allkeys = new TokenList(m.keys());
    allkeys.sort(true);

    // create a sorted list of the new keys
    final TokenList nkeys = new TokenList(m.size());
    int p = 0;
    for(final byte[] k : allkeys) {
      p = get(k, p, size - 1);
      if(p < 0) {
        p = -(p + 1);
        // key does not exist and needs to be inserted
        nkeys.add(k);
      } else {
        // key exists: append the new ids
        appendIds(p, k, m.get(k));
      }
    }

    // insert new keys
    for(int j = nkeys.size() - 1, i = size - 1, pos = size + j; j >= 0; --j) {
      // search each key in the index
      final byte[] k = nkeys.get(j);
      int ins = get(k, 0, i);

      if(ins < 0) {
        // key does not exist and needs to be inserted
        ins = -(ins + 1);

        // shift all bigger keys to the right
        while(i >= ins) {
          idxr.write5(pos * 5L, idxr.read5(i * 5L));
          ctext.set(ctext.get(i--), pos--);
        }

        final int[] ids = m.get(k).sort().toArray();
        for(int l = ids.length - 1; l > 0; --l) ids[l] -= ids[l - 1];

        // add the new key and its ids
        final long newpos = idxl.length();
        idxl.writeNums(newpos, ids);
        idxr.write5(pos * 5L, newpos);

        // cache the key token
        ctext.set(k, pos--);
        // [DP] should the entry be added to the cache?
      } else {
        throw new IllegalStateException("Key should not exists");
      }
    }

    size += nkeys.size();
  }

  /**
   * Add record ids to an index entry.
   * @param ix index of the key
   * @param txt key
   * @param nids list of record ids to add
   */
  private void appendIds(final int ix, final byte[] txt, final IntList nids) {
    final int numnew = nids.size();

    final long oldpos = idxr.read5(ix * 5L);
    final int numold = idxl.readNum(oldpos);
    final int[] ids = new int[numold + numnew];

    // read the old ids
    int pid = 0;
    for(int i = 0; i < numold; ++i) {
      final int v = idxl.readNum();
      pid += v;
      ids[i] = v;
    }

    // append the new ids - they are bigger than the old ones
    final int[] newids = nids.sort().toArray();
    for(int l = newids.length - 1; l > 0; --l) newids[l] -= newids[l - 1];
    newids[0] -= pid;
    System.arraycopy(newids, 0, ids, numold, newids.length);

    final long newpos = idxl.length();
    idxl.writeNums(newpos, ids);
    idxr.write5(ix * 5L, newpos);

    // check if txt is cached and update the cache entry
    final int cacheid = cache.id(txt);
    if(cacheid > 0)
      cache.update(cacheid, ids.length, newpos + Num.length(ids.length));
  }

  /**
   * Remove record from the index.
   * @param o old record key
   * @param n new record key
   * @param id record id
   */
  public void replace(final byte[] o, final byte[] n, final int id) {
    // delete the entry from the old key
    final IntList arr = new IntList(new int[] { id});
    final int keyIndex = deleteIds(o, arr);
    if(keyIndex >= 0) {
      arr.set(keyIndex, 0);
      deleteKeys(arr);
    }

    // add the entry to the new key
    insertId(n, id);
  }

  /**
   * Add a text entry to the index.
   * @param txt text to index
   * @param id id value
   */
  private void insertId(final byte[] txt, final int id) {
    // search for the key
    int ix = get(txt);
    if(ix < 0) {
      // key does not exist, create a new entry with the id
      ix = -(ix + 1);

      // shift all entries with bigger keys to the right
      for(int i = size; i > ix; --i)
        idxr.write5(i * 5L, idxr.read5((i - 1) * 5L));

      final long newpos = idxl.length();
      idxl.writeNums(newpos, new int[] { id});
      idxr.write5(ix * 5L, newpos);

      // [DP] should the entry be added to the cache?
      ++size;
      ctext.add(txt, ix);
    } else {
      // add id to the list of ids in the index node
      // read the position of the id-list in idxl
      final long pos = idxr.read5(ix * 5L);
      // read the number of ids in the list
      final int num = idxl.readNum(pos);

      final IntList ids = new IntList(num + 1);
      boolean added = false;
      int cid = 0;
      // read all elements from the list: the first is a text node id; then
      // next value is the difference between the id and the previous id
      for(int i = 0; i < num; ++i) {
        int v = idxl.readNum();
        if(id < cid + v) {
          // add the difference between the previous id and the new id
          ids.add(id - cid);
          // decrement the difference to the next id
          v -= id - cid;
          cid = id;
          added = true;
        }
        // [DP] if insert performance is slow, dont't make the check
        // add the next id only if it hasn't been deleted
        ids.add(v);
        cid += v;
      }

      if(!added) ids.add(id - cid);

      final long newpos = idxl.length();
      idxl.writeNums(idxl.length(), ids.toArray());
      idxr.write5(ix * 5L, newpos);

      // check if txt is cached and update the cache entry
      final int cacheid = cache.id(txt);
      if(cacheid > 0) cache.update(cacheid, ids.size(),
          newpos + Num.length(ids.size()));
    }
  }

  /**
   * Delete records from the index.
   * @param m a set of <key, id-list> pairs
   */
  public void delete(final TokenObjMap<IntList> m) {
    final IntList empty = new IntList(m.size());
    for(final byte[] key : m) {
      final int keyIndex = deleteIds(key, m.get(key));
      if(keyIndex >= 0) empty.add(keyIndex);
    }
    deleteKeys(empty);
  }

  /**
   * Remove record ids from the index.
   * @param key record key
   * @param ids list of record ids to delete
   * @return the position of the key, if all ids for the key have been deleted
   *         {@code -1} otherwise
   */
  private int deleteIds(final byte[] key, final IntList ids) {
    // safety check
    final int numdel = ids.size();
    if(numdel == 0) return -1;

    // find the key position in the index
    // [DP] use limited binary search
    final int ix = get(key);
    if(ix < 0) return -1;

    // read the position of the id-list in idxl
    final long pos = idxr.read5(ix * 5L);

    // read the number of ids in the list
    final int numold = idxl.readNum(pos);
    final int numnew = numold - numdel;
    if(numnew == 0) {
      // the list is empty; the key itself will be deleted
      cache.delete(key);
      return ix;
    }

    final int[] nids = new int[numnew];
    final int[] oids = ids.sort().toArray();

    // read each element from the list
    for(int i = 0, j = 0, cid = 0, pid = 0; i < numnew;) {
      cid += idxl.readNum();
      if(j < numdel && oids[j] == cid) {
        // there are more ids to delete and the current id must be deleted
        // skip the current id from adding to the new list
        ++j;
      } else {
        // add the difference between previous and current id
        nids[i++] = cid - pid;
        pid = cid;
      }
    }

    idxl.writeNums(pos, nids);
    // update the cache entry, if the key is cached
    final int cacheid = cache.id(key);
    if(cacheid > 0)
      cache.update(cacheid, nids.length, pos + Num.length(nids.length));

    return -1;
  }

  /**
   * Delete keys from the index.
   * @param keys list of key positions to delete
   */
  private void deleteKeys(final IntList keys) {
    final int num = keys.size();
    if(num == 0) return;

    keys.sort();

    // shift all keys to the left, skipping the ones which have to be deleted
    int j = 0;
    for(int pos = keys.get(j++), i = pos + 1; i < size; ++i) {
      if(j < num && i == keys.get(j)) {
        ++j;
      } else {
        idxr.write5(pos * 5L, idxr.read5(i * 5L));
        ctext.set(ctext.get(i), pos++);
      }
    }

    // reduce the size of the index
    size -= j;
  }
}
