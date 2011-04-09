package org.basex.build;

import static org.basex.core.Text.*;
import static org.basex.build.BuildText.*;
import static org.basex.util.Token.*;
import java.io.IOException;
import org.basex.core.Progress;
import org.basex.core.Prop;
import org.basex.data.Data;
import org.basex.data.MetaData;
import org.basex.data.Namespaces;
import org.basex.data.PathSummary;
import org.basex.index.Names;
import org.basex.io.IO;
import org.basex.util.Atts;
import org.basex.util.Performance;
import org.basex.util.Util;

/**
 * This class provides an interface for building database instances.
 * The specified {@link Parser} sends events to this class whenever nodes
 * are to be added or closed. The builder implementation decides whether
 * the nodes are stored on disk or kept in memory.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public abstract class Builder extends Progress {
  /** Meta data on built database. */
  public MetaData meta;

  /** Tree structure. */
  protected final PathSummary path = new PathSummary();
  /** Namespace index. */
  protected final Namespaces ns = new Namespaces();
  /** Parser instance. */
  protected final Parser parser;
  /** Property instance. */
  protected final Prop prop;
  /** Tag name index. */
  protected final Names tags;;
  /** Attribute name index. */
  protected final Names atts;

  /** Number of cached size values. */
  protected int ssize;
  /** Currently stored size value. */
  protected int spos;

  /** Parent stack. */
  private final int[] preStack = new int[IO.MAXHEIGHT];
  /** Tag stack. */
  private final int[] tagStack = new int[IO.MAXHEIGHT];
  /** Size stack. */
  private boolean inDoc;
  /** Current tree height. */
  private int lvl;
  /** Element counter. */
  private int c;

  /**
   * Constructor.
   * @param parse parser
   * @param pr properties
   */
  protected Builder(final Parser parse, final Prop pr) {
    parser = parse;
    prop = pr;
    final int cats = pr.num(Prop.CATEGORIES);
    tags = new Names(cats);
    atts = new Names(cats);
  }

  // Public Methods ============================================================

  /**
   * Builds the database.
   * @param name name of database
   * @throws IOException I/O exception
   */
  protected final void parse(final String name) throws IOException {
    final Performance perf = Util.debug ? new Performance() : null;
    Util.debug(tit() + DOTS);

    // add document node and parse document
    parser.parse(this);
    if(lvl != 0) error(DOCOPEN, parser.detail(), tags.key(tagStack[lvl]));

    meta.lastid = meta.size - 1;
    // no nodes inserted: add default document node
    if(meta.size == 0) {
      startDoc(token(name));
      endDoc();
      setSize(0, meta.size);
    }

    Util.gc(perf);
  }

  /**
   * Opens a document node.
   * @param doc document name
   * @throws IOException I/O exception
   */
  public final void startDoc(final byte[] doc) throws IOException {
    preStack[lvl++] = meta.size;
    if(meta.pathindex) path.add(0, lvl, Data.DOC);
    addDoc(doc);
    ns.open();
  }

  /**
   * Closes a document node.
   * @throws IOException I/O exception
   */
  public final void endDoc() throws IOException {
    final int pre = preStack[--lvl];
    setSize(pre, meta.size - pre);
    meta.ndocs++;
    ns.close(meta.size);
    inDoc = false;
  }

  /**
   * Adds a new namespace; called by the building instance.
   * @param pref the namespace prefix
   * @param uri namespace uri
   */
  public final void startNS(final byte[] pref, final byte[] uri) {
    ns.add(pref, uri, meta.size);
  }

  /**
   * Opens a new element node.
   * @param tag tag name
   * @param att attributes
   * @return preValue of the created node
   * @throws IOException I/O exception
   */
  public final int startElem(final byte[] tag, final Atts att)
      throws IOException {

    final int pre = addElem(tag, att);
    if(meta.height < ++lvl) meta.height = lvl;
    return pre;
  }

  /**
   * Stores an empty element.
   * @param tag tag name
   * @param att attributes
   * @throws IOException I/O exception
   */
  public final void emptyElem(final byte[] tag, final Atts att)
      throws IOException {

    addElem(tag, att);
    ns.close(preStack[lvl]);
  }

  /**
   * Closes an element.
   * @param tag tag name
   * @throws IOException I/O exception
   */
  public final void endElem(final byte[] tag) throws IOException {
    checkStop();

    if(--lvl == 0 || tags.id(tag) != tagStack[lvl])
      error(CLOSINGTAG, parser.detail(), tag, tags.key(tagStack[lvl]));

    final int pre = preStack[lvl];
    setSize(pre, meta.size - pre);
    ns.close(pre);
  }

  /**
   * Stores a text node.
   * @param txt text value
   * @throws IOException I/O exception
   */
  public final void text(final byte[] txt) throws IOException {
    // chop whitespaces in text nodes
    final byte[] t = meta.chop ? trim(txt) : txt;

    // check if text appears before or after root node
    final boolean ignore = !inDoc || lvl == 1;
    if((meta.chop && t.length != 0 || !ws(t)) && ignore)
      error(inDoc ? AFTERROOT : BEFOREROOT, parser.detail());

    if(t.length != 0 && !ignore) addText(t, Data.TEXT);
  }

  /**
   * Stores a comment.
   * @param com comment text
   * @throws IOException I/O exception
   */
  public final void comment(final byte[] com) throws IOException {
    addText(com, Data.COMM);
  }

  /**
   * Stores a processing instruction.
   * @param pi processing instruction name and value
   * @throws IOException I/O exception
   */
  public final void pi(final byte[] pi) throws IOException {
    addText(pi, Data.PI);
  }

  /**
   * Sets the document encoding.
   * @param enc encoding
   */
  public final void encoding(final String enc) {
    meta.encoding = enc.equals(UTF8) || enc.equals(UTF82) ? UTF8 : enc;
  }

  // PROGRESS INFORMATION =====================================================

  @Override
  public final String tit() {
    return PROGCREATE;
  }

  @Override
  public final String det() {
    return spos == 0 ? parser.detail() : DBFINISH;
  }

  @Override
  public final double prog() {
    return spos == 0 ? parser.progress() : (double) spos / ssize;
  }

  // ABSTRACT METHODS =========================================================

  /**
   * Builds the database by running the specified parser.
   * @param db name of database
   * @return data database instance
   * @throws IOException I/O exception
   */
  public abstract Data build(final String db) throws IOException;

  /**
   * Closes open references.
   * @throws IOException I/O exception
   */
  public abstract void close() throws IOException;

  /**
   * Adds a document node to the database.
   * @param txt name of the document
   * @throws IOException I/O exception
   */
  protected abstract void addDoc(byte[] txt) throws IOException;

  /**
   * Adds an element node to the database. This method stores a preliminary
   * size value; if this node has further descendants, {@link #setSize} must
   * be called to set the final size value.
   * @param n the tag name reference
   * @param u namespace uri reference
   * @param dis distance to parent
   * @param as number of attributes
   * @param ne namespace flag
   * @throws IOException I/O exception
   */
  protected abstract void addElem(int n, int u, int dis, int as, boolean ne)
    throws IOException;

  /**
   * Adds an attribute to the database.
   * @param n attribute name
   * @param v attribute value
   * @param dis distance to parent
   * @param u namespace uri reference
   * @throws IOException I/O exception
   */
  protected abstract void addAttr(int n, byte[] v, int dis, int u)
    throws IOException;

  /**
   * Adds a text node to the database.
   * @param tok the token to be added (tag name or content)
   * @param dis distance to parent
   * @param kind the node kind
   * @throws IOException I/O exception
   */
  protected abstract void addText(byte[] tok, int dis, byte kind)
    throws IOException;

  /**
   * Stores a size value to the specified table position.
   * @param pre pre reference
   * @param val value to be stored
   * @throws IOException I/O exception
   */
  protected abstract void setSize(int pre, int val) throws IOException;

  // PRIVATE METHODS ==========================================================

  /**
   * Adds an element node to the storage.
   * @param tag tag name
   * @param att attributes
   * @return pre value of the created node
   * @throws IOException I/O exception
   */
  private int addElem(final byte[] tag, final Atts att) throws IOException {
    // get tag reference
    int n = tags.index(tag, null, true);

    if(meta.pathindex) path.add(n, lvl, Data.ELEM);

    // cache pre value
    final int pre = meta.size;
    // remember tag id and parent reference
    tagStack[lvl] = n;
    preStack[lvl] = pre;

    // get and store element references
    final int dis = lvl != 0 ? pre - preStack[lvl - 1] : 1;
    final int as = att.size;
    final boolean ne = ns.open();
    int u = ns.uri(tag, true);
    addElem(dis, n, Math.min(IO.MAXATTS, as + 1), u, ne);

    // get and store attribute references
    for(int a = 0; a < as; ++a) {
      n = atts.index(att.key[a], att.val[a], true);
      u = ns.uri(att.key[a], false);
      if(meta.pathindex) path.add(n, lvl + 1, Data.ATTR);
      addAttr(n, att.val[a], Math.min(IO.MAXATTS, a + 1), u);
    }

    if(lvl != 0) {
      if(lvl > 1) {
        // set leaf node information in index
        tags.stat(tagStack[lvl - 1]).leaf = false;
      } else if(inDoc) {
        // don't allow more than one root node
        error(MOREROOTS, parser.detail(), tag);
      }
    }
    if(meta.size != 1) inDoc = true;

    if(Util.debug && (c++ & 0x7FFFF) == 0) Util.error(".");

    // check if data ranges exceed database limits,
    // based on the storage details in {@link Data}
    limit(tags.size(), 0x8000, LIMITTAGS);
    limit(atts.size(), 0x8000, LIMITATTS);
    limit(ns.size(), 0x100, LIMITNS);
    if(meta.size < 0) limit(0, 0, LIMITRANGE);
    return pre;
  }

  /**
   * Checks a value limit and optionally throws an exception.
   * @param v value
   * @param l limit
   * @param m message
   * @throws BuildException build exception
   */
  private void limit(final int v, final int l, final String m)
      throws BuildException {
    if(v >= l) error(m, parser.detail(), l);
  }

  /**
   * Adds a simple text, comment or processing instruction to the database.
   * @param txt the value to be added
   * @param kind the node type
   * @throws IOException I/O exception
   */
  private void addText(final byte[] txt, final byte kind)
      throws IOException {

    // text node processing for statistics
    if(kind == Data.TEXT) tags.index(tagStack[lvl - 1], txt);
    if(meta.pathindex) path.add(0, lvl, kind);
    addText(txt, lvl == 0 ? 1 : meta.size - preStack[lvl - 1], kind);
  }

  /**
   * Throws an error message.
   * @param m message
   * @param e message extension
   * @throws BuildException build exception
   */
  private static void error(final String m, final Object... e)
      throws BuildException {
    throw new BuildException(m, e);
  }
}
