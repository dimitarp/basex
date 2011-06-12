package org.basex.data;

import static org.basex.data.DataText.*;
import static org.basex.util.Token.*;
import java.io.File;
import java.io.IOException;
import org.basex.build.DiskBuilder;
import org.basex.core.Prop;
import org.basex.index.FTIndex;
import org.basex.index.Index;
import org.basex.index.IndexToken.IndexType;
import org.basex.index.Names;
import org.basex.index.DiskValues;
import org.basex.io.DataAccess;
import org.basex.io.DataInput;
import org.basex.io.DataOutput;
import org.basex.io.IO;
import org.basex.io.TableDiskAccess;
import org.basex.util.Compress;
import org.basex.util.IntList;
import org.basex.util.Token;
import org.basex.util.TokenObjMap;
import org.basex.util.Util;

/**
 * This class stores and organizes the database table and the index structures
 * for textual content in a compressed disk structure.
 * The table mapping is documented in {@link Data}.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 * @author Tim Petrowsky
 */
public final class DiskData extends Data {
  /** Text compressor. */
  private final Compress comp = new Compress();
  /** Texts access file. */
  private DataAccess texts;
  /** Values access file. */
  private DataAccess values;
  /** Texts buffered for subsequent index updates. */
  TokenObjMap<IntList> txts;
  /** Attribute values buffered for subsequent index updates. */
  TokenObjMap<IntList> atvs;

  /**
   * Default constructor.
   * @param db name of database
   * @param pr database properties
   * @throws IOException IO Exception
   */
  public DiskData(final String db, final Prop pr) throws IOException {
    meta = new MetaData(db, pr);

    final int cats = pr.num(Prop.CATEGORIES);
    final DataInput in = new DataInput(meta.file(DATAINFO));
    try {
      // read meta data and indexes
      meta.read(in);
      while(true) {
        final String k = Token.string(in.readBytes());
        if(k.isEmpty()) break;
        if(k.equals(DBTAGS))      tagindex = new Names(in, cats);
        else if(k.equals(DBATTS)) atnindex = new Names(in, cats);
        else if(k.equals(DBPATH)) pthindex = new PathSummary(in);
        else if(k.equals(DBNS))   ns = new Namespaces(in);
        else if(k.equals(DBDOCS)) meta.docindex = docindex.read(in);
      }
      // open data and indexes
      init();
      if(meta.textindex) txtindex = new DiskValues(this, true);
      if(meta.attrindex) atvindex = new DiskValues(this, false);
      if(meta.ftindex)   ftxindex = FTIndex.get(this, meta.wildcards);
    } catch(final IOException ex) {
      throw ex;
    } finally {
      try { in.close(); } catch(final IOException ex) { }
    }
  }

  /**
   * Internal database constructor, called from {@link DiskBuilder#build}.
   * @param md meta data
   * @param nm tags
   * @param at attributes
   * @param ps path summary
   * @param n namespaces
   * @throws IOException IO Exception
   */
  public DiskData(final MetaData md, final Names nm, final Names at,
      final PathSummary ps, final Namespaces n) throws IOException {

    meta = md;
    tagindex = nm;
    atnindex = at;
    pthindex = ps;
    ns = n;
    init();
    flush();
  }

  @Override
  public void init() throws IOException {
    table = new TableDiskAccess(meta, DATATBL);
    texts = new DataAccess(meta.file(DATATXT));
    values = new DataAccess(meta.file(DATAATV));
    super.init();
    // if the ID -> PRE mapping is available restore it from disk
    final File idpfile = meta.file(DATAIDP);
    idmap = idpfile.exists() && idpfile.length() > 0L ?
        new IdPreMap(idpfile) :
        new IdPreMap(meta.lastid);
  }

  /**
   * Writes all meta data to disk.
   * @throws IOException I/O exception
   */
  private void write() throws IOException {
    final DataOutput out = new DataOutput(meta.file(DATAINFO));
    meta.write(out);
    out.writeString(DBTAGS);
    tagindex.write(out);
    out.writeString(DBATTS);
    atnindex.write(out);
    out.writeString(DBPATH);
    pthindex.write(out);
    out.writeString(DBNS);
    ns.write(out);
    out.writeString(DBDOCS);
    docindex.write(this, out);
    out.write(0);
    out.close();
  }

  @Override
  public synchronized void flush() {
    try {
      if(meta.dirty) write();
      table.flush();
      texts.flush();
      values.flush();
      if(txtindex != null) ((DiskValues) txtindex).flush();
      if(atvindex != null) ((DiskValues) atvindex).flush();
      idmap.write(meta.file(DATAIDP));
      meta.dirty = false;
    } catch(final IOException ex) {
      Util.stack(ex);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    flush();
    table.close();
    texts.close();
    values.close();
    closeIndex(IndexType.TEXT);
    closeIndex(IndexType.ATTRIBUTE);
    closeIndex(IndexType.FULLTEXT);
  }

  @Override
  public synchronized void closeIndex(final IndexType type) throws IOException {
    switch(type) {
      case TEXT:
        if(txtindex != null) { txtindex.close(); txtindex = null; }
        break;
      case ATTRIBUTE:
        if(atvindex != null) { atvindex.close(); atvindex = null; }
        break;
      case FULLTEXT:
        if(ftxindex != null) { ftxindex.close(); ftxindex = null; }
        break;
      default:
        // path index will not be closed
        break;
    }
  }

  @Override
  public void setIndex(final IndexType type, final Index index) {
    meta.dirty = true;
    switch(type) {
      case TEXT:      txtindex = index; break;
      case ATTRIBUTE: atvindex = index; break;
      case FULLTEXT:  ftxindex = index; break;
      case PATH:      pthindex = (PathSummary) index; break;
      default: break;
    }
  }

  @Override
  public byte[] text(final int pre, final boolean text) {
    final long o = textOff(pre);
    return num(o) ? Token.token((int) o) : txt(o, text);
  }

  @Override
  public long textItr(final int pre, final boolean text) {
    final long o = textOff(pre);
    return num(o) ? o & IO.OFFNUM - 1 : Token.toLong(txt(o, text));
  }

  @Override
  public double textDbl(final int pre, final boolean text) {
    final long o = textOff(pre);
    return num(o) ? o & IO.OFFNUM - 1 : Token.toDouble(txt(o, text));
  }

  @Override
  public int textLen(final int pre, final boolean text) {
    final long o = textOff(pre);
    if(num(o)) return Token.numDigits((int) o);
    final DataAccess da = text ? texts : values;
    final int l = da.readNum(o & IO.OFFCOMP - 1);
    // compressed: next number contains number of compressed bytes
    return cpr(o) ? da.readNum() : l;
  }

  /**
   * Returns a text (text, comment, pi) or attribute value.
   * @param o text offset
   * @param text text or attribute flag
   * @return text
   */
  private byte[] txt(final long o, final boolean text) {
    final byte[] txt = (text ? texts : values).readToken(o & IO.OFFCOMP - 1);
    return cpr(o) ? comp.unpack(txt) : txt;
  }

  /**
   * Returns true if the specified value contains a number.
   * @param o offset
   * @return result of check
   */
  private static boolean num(final long o) {
    return (o & IO.OFFNUM) != 0;
  }

  /**
   * Returns true if the specified value references a compressed token.
   * @param o offset
   * @return result of check
   */
  private static boolean cpr(final long o) {
    return (o & IO.OFFCOMP) != 0;
  }

  // UPDATE OPERATIONS ========================================================
  @Override
  protected void text(final int pre, final byte[] val, final boolean txt) {
    // update indexes
    final int id = id(pre);
    final byte[] oldval = text(pre, txt);
    final DiskValues index = (DiskValues) (txt ? txtindex : atvindex);
    if(index != null) index.replace(oldval, val, id);

    final long v = Token.toSimpleInt(val);
    if(v != Integer.MIN_VALUE) {
      // integer values are stored directly into the table
      textOff(pre, v | IO.OFFNUM);
    } else {
      final DataAccess da = txt ? texts : values;
      final byte[] pack = comp.pack(val);

      // old text
      final long old = textOff(pre);
      // old offset
      long o = old & IO.OFFCOMP - 1;

      if(!num(old)) {
        // handle non-numeric entry
        final int len = da.readNum(o);
        if(da.pos() + len == da.length()) {
          // set new file length if entry is placed last
          da.length(da.pos() + pack.length);
        } else if(pack.length > len) {
          // otherwise, if new text is longer than the old, append text
          o = da.length();
        }
      } else {
        // if old text was numeric, append text at the end
        o = da.length();
      }

      da.writeBytes(o, pack);
      textOff(pre, o | (pack == val ? 0 : IO.OFFCOMP));
    }
  }

  @Override
  protected void indexBegin() {
    txts = new TokenObjMap<IntList>();
    atvs = new TokenObjMap<IntList>();
  }

  @Override
  protected void indexEnd() {
    // update all indexes in parallel
    // [DP] Full-text index updates: update the existing indexes
    final Thread txtupdater = txts.size() > 0 ? runIndexInsert(txtindex, txts)
        : null;
    final Thread atvupdater = atvs.size() > 0 ? runIndexInsert(atvindex, atvs)
        : null;

    // wait for all tasks to finish
    try {
      if(txtupdater != null) txtupdater.join();
      if(atvupdater != null) atvupdater.join();
    } catch(InterruptedException e) { Util.stack(e); }
  }

  @Override
  protected long index(final byte[] txt, final int id, final boolean text) {
    final DataAccess da;
    final TokenObjMap<IntList> m;

    if(text) {
      da = texts;
      m = meta.textindex ? txts : null;
    } else {
      da = values;
      m = meta.attrindex ? atvs : null;
    }

    // add text to map to index later
    if(m != null && len(txt) <= MAXLEN) {
      final IntList ids;
      final int hash = m.id(txt);
      if(hash == 0) {
        ids = new IntList();
        m.add(txt, ids);
      } else {
        ids = m.value(hash);
      }
      ids.add(id);
    }

    // add text to text file
    final long off = da.length();
    da.writeBytes(off, txt);
    return off;
  }

  @Override
  protected void indexDelete(final int pre, final int size) {
    if(!(meta.textindex || meta.attrindex)) return;

    // collect all keys and ids
    txts = new TokenObjMap<IntList>();
    atvs = new TokenObjMap<IntList>();
    final int l = pre + size;
    for(int p = pre; p < l; ++p) {
      final int k = kind(p);
      final boolean isAttr = k == ATTR;
      // consider nodes which are attribute, text, comment, or proc. instruction
      if(isAttr || k == TEXT || k == COMM || k == PI) {
        final byte[] key = text(p, !isAttr);
        if(len(key) <= MAXLEN) {
          final IntList ids;
          final TokenObjMap<IntList> m = isAttr ? atvs : txts;
          final int hash = m.id(key);
          if(hash == 0) {
            ids = new IntList();
            m.add(key, ids);
          } else {
            ids = m.value(hash);
          }
          ids.add(id(p));
        }
      }
    }

    // update all indexes in parallel
    // [DP] Full-text index updates: update the existing indexes
    final Thread txtupdater = txts.size() > 0 ? runIndexDelete(txtindex, txts)
        : null;
    final Thread atvupdater = atvs.size() > 0 ? runIndexDelete(atvindex, atvs)
        : null;

    // wait for all tasks to finish
    try {
      if(txtupdater != null) txtupdater.join();
      if(atvupdater != null) atvupdater.join();
    } catch(InterruptedException e) { Util.errln(e); }
  }

  @Override
  protected void update() {
    meta.update();
  }

  /**
   * Start a new thread which inserts records into an index.
   * @param ix index
   * @param m records to be inserted
   * @return the new thread
   */
  private Thread runIndexInsert(final Index ix, final TokenObjMap<IntList> m) {
    final Thread t = new Thread(new Runnable() { @Override public void run() {
      final org.basex.util.Performance p = new org.basex.util.Performance();
      ((DiskValues) ix).index(m);
      Util.errln("Index insert finished in " + p);
    }});
    t.start();
    return t;
  }

  /**
   * Start a new thread which deletes records from an index.
   * @param ix index
   * @param m records to be deleted
   * @return the new thread
   */
  private Thread runIndexDelete(final Index ix, final TokenObjMap<IntList> m) {
    final Thread t = new Thread(new Runnable() { @Override public void run() {
      final org.basex.util.Performance p = new org.basex.util.Performance();
      ((DiskValues) ix).delete(m);
      Util.errln("Index delete finished in " + p);
    }});
    t.start();
    return t;
  }
}
