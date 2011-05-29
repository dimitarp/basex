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

/**
 * This class provides access to attribute values and text contents
 * stored on disk.
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
      final int oc = idxl.readNum(idxr.read5(m * 5L));
      if(stats.adding(oc)) stats.add(data.text(idxl.readNum(), text));
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
    final int num = idxl.readNum(pos);
    cache.add(it.get(), num, pos + Num.len(num));

    return num;
  }

  /**
   * Returns next id values.
   * @return compressed id values
   */
  byte[] nextIDs() {
    if(idxr.pos() >= idxr.length()) return EMPTY;
    final int s = idxl.read4();
    final long v = idxr.read5(idxr.pos());
    return idxl.readBytes(v, s);
  }

  /**
   * Iterator method.
   * @param s number of pre values
   * @param ps offset
   * @return iterator
   */
  private IndexIterator iter(final int s, final long ps) {
    final IntList pres = new IntList(s);
    long p = ps;
    for(int l = 0, v = 0; l < s; ++l) {
      v += idxl.readNum(p);
      p = idxl.pos();
      final int pre = data.pre(v);
      if(pre >= 0) pres.add(pre);
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
      int pre = data.pre(id);
      if(pre >= 0) {
        final double v = data.textDbl(pre, text);

        if(v >= min && v <= max) {
          // value is in range
          for(int d = 0; d < ds; ++d) {
            pre = data.pre(id);
            if(pre >= 0) pres.add(pre);
            id += idxl.readNum();
          }
        } else if(simple && v > max && data.textLen(pre, text) == len) {
          // if limits are integers, if min, max and current value have the same
          // string length, and if current value is larger than max, test can be
          // skipped, as all remaining values will be bigger
          break;
        }
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
    final int num = idxl.readNum(pos);
    for(int i = 0, v = 0; i < num; ++i) {
      v += idxl.readNum();
      final int pre = data.pre(v);
      if(pre >= 0) return pre;
    }
    return -1;
  }

  /**
   * Binary search for key in the {@link #idxr}.
   * @param key token to be found
   * @return if the key is found: index of the key
   *         else: -(insertion point - 1)
   */
  private int get(final byte[] key) {
    // [DP] Refactor!
    int l = 0, h = size - 1;
    while(l <= h) {
      int m = l + h >>> 1;
      int pre = firstpre(idxr.read5(m * 5L));

      if(pre < 0) {
        // try to find the next non-negative pre to the left
        for(int i = m - 1; i >= l; --i) {
          pre = firstpre(idxr.read5(i * 5L));
          if(pre >= 0) {
            m = i;
            break;
          }
        }
      }

      if(pre < 0) {
        // try to find the next non-negative pre to the right
        for(int i = m + 1; i <= h; ++i) {
          pre = firstpre(idxr.read5(i * 5L));
          if(pre >= 0) {
            m = i;
            break;
          }
        }
      }

      if(pre < 0) break;

      byte[] txt = ctext.get(m);
      if(txt == null) {
        txt = data.text(pre, text);
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
   * Add a text entry to the index.
   * @param txt text to index
   * @param id id value
   */
  public void index(final byte[] txt, final int id) {
    // search for the key
    int ix = get(txt);
    if(ix < 0) {
      // key does not exist, create a new entry with the id
      ix = -(ix + 1);

      // shift all entries with bigger keys to the right
      for(int i = size; i > ix; --i)
        idxr.write5(i * 5L, idxr.read5((i - 1) * 5L));

      final long newpos = idxl.length();
      idxl.writeNums(idxl.length(), new int[] { id});
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
        // add the next id only if it hasn't been deleted
        if(data.pre(cid + v) >= 0) {
          ids.add(v);
          cid += v;
        }
      }

      if(!added) ids.add(id - cid);

      final long newpos = idxl.length();
      idxl.writeNums(idxl.length(), ids.toArray());
      idxr.write5(ix * 5L, newpos);

      // check if txt is cached and update the cache entry
      final int cacheid = cache.id(txt);
      if(cacheid > 0)
        cache.update(cacheid, ids.size(), newpos + Num.len(ids.size()));
    }
  }

  /**
   * Remove record from the index.
   * @param txt record key
   * @param id record id
   */
  public void indexDelete(final byte[] txt, final int id) {
    final int ix = get(txt);
    if(ix < 0) return;

    // read the position of the id-list in idxl
    final long pos = idxr.read5(ix * 5L);

    // read the number of ids in the list
    final int num = idxl.readNum(pos);
    final IntList ids = new IntList(num);
    boolean unchanged = true;
    int cid = 0;
    for(int i = 0; i < num || id < cid; ++i) {
      int v = idxl.readNum();
      if(unchanged) {
        if(id == cid + v) {
          unchanged = false;
          if(i == num - 1) break;
          v += idxl.readNum();
        } else {
          cid += v;
        }
      }
      ids.add(v);
    }

    if(!unchanged) {
      idxl.writeNums(pos, ids.toArray());
      // check if txt is cached and update the cache entry
      final int cacheid = cache.id(txt);
      if(cacheid > 0)
        cache.update(cacheid, ids.size(), pos + Num.len(ids.size()));
    }
  }
}
