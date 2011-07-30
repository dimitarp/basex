package org.basex.data;

import org.basex.core.Prop;
import org.basex.index.IdPreMap;
import org.basex.index.Index;
import org.basex.index.IndexToken.IndexType;
import org.basex.index.path.PathSummary;
import org.basex.index.value.MemValues;
import org.basex.index.Names;
import org.basex.io.random.TableMemAccess;
import org.basex.util.Token;

/**
 * This class stores and organizes the database table and the index structures
 * for textual content in a compressed memory structure.
 * The table mapping is documented in {@link Data}.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class MemData extends Data {
  /**
   * Constructor.
   * @param tag tag index
   * @param att attribute name index
   * @param n namespaces
   * @param s path summary
   * @param pr database properties
   */
  public MemData(final Names tag, final Names att, final Namespaces n,
      final PathSummary s, final Prop pr) {

    meta = new MetaData(pr);
    idmap = new IdPreMap(meta.lastid);
    table = new TableMemAccess(meta);
    txtindex = new MemValues(idmap);
    atvindex = new MemValues(idmap);
    tagindex = tag;
    atnindex = att;
    ns = n;
    pthindex = s;
  }

  /**
   * Constructor, adopting meta data from the specified database.
   * @param data data reference
   */
  public MemData(final Data data) {
    this(data.tagindex, data.atnindex, new Namespaces(),
         data.pthindex, data.meta.prop);
  }

  /**
   * Constructor, creating a new, empty database.
   * @param pr property reference
   */
  public MemData(final Prop pr) {
    this(new Names(0), new Names(0), new Namespaces(), new PathSummary(), pr);
  }

  @Override
  public void flush() { }

  @Override
  public void close() { }

  @Override
  public void closeIndex(final IndexType type) { }

  @Override
  public void setIndex(final IndexType type, final Index index) { }

  @Override
  public byte[] text(final int pre, final boolean text) {
    return ((MemValues) (text ? txtindex : atvindex)).key((int) textOff(pre));
  }

  @Override
  public long textItr(final int pre, final boolean text) {
    return Token.toLong(text(pre, text));
  }

  @Override
  public double textDbl(final int pre, final boolean text) {
    return Token.toDouble(text(pre, text));
  }

  @Override
  public int textLen(final int pre, final boolean text) {
    return text(pre, text).length;
  }

  // UPDATE OPERATIONS ========================================================

  @Override
  public void updateText(final int pre, final byte[] val, final int kind) {
    final boolean txt = kind != ATTR;
    final int id = id(pre);
    ((MemValues) (txt ? txtindex : atvindex)).delete(text(pre, txt), id);
    textOff(pre, index(val, id, kind));
  }

  @Override
  protected long index(final byte[] txt, final int id, final int kind) {
    return ((MemValues) (kind == ATTR ? atvindex : txtindex)).index(txt, id);
  }

  @Override
  protected void indexDelete(final int pre, final int size) {
    final int l = pre + size;
    for(int p = pre; p < l; ++p) {
      final int k = kind(p);
      final boolean isAttr = k == ATTR;
      // skip nodes which are not attribute, text, comment, or proc. instruction
      if(isAttr || k == TEXT || k == COMM || k == PI) {
        final byte[] key = text(p, !isAttr);
        ((MemValues) (isAttr ? atvindex : txtindex)).delete(key, id(p));
      }
    }
  }

  @Override
  protected void update() {
    meta.update();
  }
}
