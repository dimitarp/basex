package org.basex.query.item;

import static org.basex.query.QueryText.*;
import java.io.IOException;

import org.basex.io.serial.Serializer;
import org.basex.query.iter.NodeCache;
import org.basex.query.util.NSGlobal;
import org.basex.util.Atts;
import static org.basex.util.Token.*;
import org.basex.util.Token;
import org.basex.util.hash.TokenMap;
import org.w3c.dom.Attr;
import org.w3c.dom.Comment;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.ProcessingInstruction;
import org.w3c.dom.Text;

/**
 * Element node fragment.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class FElem extends FNode {
  /** Namespaces. */
  private final Atts ns;
  /** Tag name. */
  private final QNm name;
  /** Base URI. */
  private byte[] base;

  /**
   * Constructor.
   * @param n tag name
   */
  public FElem(final QNm n) {
    this(n, (Atts) null, null);
  }

  /**
   * Constructor.
   * @param n tag name
   * @param nsp namespaces
   * @param b base uri
   */
  public FElem(final QNm n, final Atts nsp, final byte[] b) {
    this(n, null, null, nsp, b);
  }

  /**
   * Constructor.
   * @param n tag name
   * @param ch children; can be {@code null}
   * @param at attributes; can be {@code null}
   * @param nsp namespaces; can be {@code null}
   * @param b base uri; can be {@code null}
   */
  public FElem(final QNm n, final NodeCache ch, final NodeCache at,
      final Atts nsp, final byte[] b) {

    super(NodeType.ELM);
    name = n;
    children = ch == null ? new NodeCache() : ch;
    atts = at == null ? new NodeCache() : at;
    base = b == null ? EMPTY : b;
    ns = nsp == null ? new Atts() : nsp;
  }

  /**
   * Constructor for DOM nodes.
   * Originally provided by Erdal Karaca.
   * @param elem DOM node
   * @param p parent reference
   * @param nss namespaces in scope
   */
  public FElem(final Element elem, final ANode p, final TokenMap nss) {
    super(NodeType.ELM);

    // general stuff
    final String nu = elem.getNamespaceURI();
    name = new QNm(token(elem.getNodeName()), nu == null ? EMPTY : token(nu));
    par = p;
    final String b = elem.getBaseURI();
    base = b == null ? EMPTY : token(b);
    children = new NodeCache();
    atts = new NodeCache();

    // attributes and namespaces
    ns = new Atts();
    final NamedNodeMap at = elem.getAttributes();
    final int as = at.getLength();

    for(int i = 0; i < as; ++i) {
      final Attr att = (Attr) at.item(i);
      final byte[] nm = token(att.getName()), uri = token(att.getValue());
      if(Token.eq(nm, XMLNS)) {
        ns.add(EMPTY, uri);
      } else if(startsWith(nm, XMLNSC)) {
        ns.add(ln(nm), uri);
      } else {
        add(new FAttr(att));
      }
    }

    // add all new namespaces
    for(int i = 0; i < ns.size; ++i) nss.add(ns.key[i], ns.val[i]);

    // no parent, so we have to add all namespaces in scope
    if(p == null) {
      nsScope(elem.getParentNode(), nss);
      for(final byte[] key : nss.keys()) {
        if(!ns.contains(key)) ns.add(key, nss.get(key));
      }
    }

    final byte[] pref = name.pref();
    final byte[] uri = name.uri().atom();
    final byte[] old = nss.get(pref);
    if(old == null || !Token.eq(uri, old)) {
      ns.add(pref, uri);
      nss.add(pref, uri);
    }

    // children
    final NodeList ch = elem.getChildNodes();
    for(int i = 0; i < ch.getLength(); ++i) {
      final Node child = ch.item(i);

      switch(child.getNodeType()) {
        case Node.TEXT_NODE:
          add(new FTxt((Text) child));
          break;
        case Node.COMMENT_NODE:
          add(new FComm((Comment) child));
          break;
        case Node.PROCESSING_INSTRUCTION_NODE:
          add(new FPI((ProcessingInstruction) child));
          break;
        case Node.ELEMENT_NODE:
          add(new FElem((Element) child, this, nss));
          break;
        default:
          break;
      }
    }
  }

  /**
   * Gathers all defined namespaces in the scope of the given DOM element.
   * @param elem DOM element
   * @param nss map
   */
  private static void nsScope(final Node elem, final TokenMap nss) {
    Node n = elem;
    // only elements can declare namespaces
    while(n != null && n instanceof Element) {
      final NamedNodeMap atts = n.getAttributes();
      final byte[] pre = token(n.getPrefix());
      if(nss.get(pre) != null) nss.add(pre, token(n.getNamespaceURI()));
      for(int i = 0, len = atts.getLength(); i < len; ++i) {
        final Attr a = (Attr) atts.item(i);
        final byte[] name = token(a.getName()), val = token(a.getValue());
        if(Token.eq(name, XMLNS)) {
          // default namespace
          if(nss.get(EMPTY) == null) nss.add(EMPTY, val);
        } else if(startsWith(name, XMLNS)) {
          // prefixed namespace
          final byte[] ln = ln(name);
          if(nss.get(ln) == null) nss.add(ln, val);
        }
      }
      n = n.getParentNode();
    }
  }

  @Override
  public byte[] base() {
    return base;
  }

  /**
   * Sets the element base.
   * @param b base
   */
  public void base(final byte[] b) {
    base = b;
  }

  @Override
  public QNm qname() {
    return name;
  }

  @Override
  public byte[] nname() {
    return name.atom();
  }

  @Override
  public Atts ns() {
    return ns;
  }

  @Override
  public void serialize(final Serializer ser) throws IOException {
    final byte[] tag = name.atom();
    ser.openElement(tag);

    if(name.hasUri()) ser.namespace(name.pref(), name.uri().atom());

    // serialize all namespaces at top level...
    if(ser.level() == 0) {
      final Atts nns = nsScope();
      for(int a = 0; a < nns.size; ++a) ser.namespace(nns.key[a], nns.val[a]);
    } else if(ns != null) {
      for(int p = ns.size - 1; p >= 0; p--) ser.namespace(ns.key[p], ns.val[p]);
    }

    // serialize attributes
    for(int n = 0; n < atts.size(); ++n) {
      final ANode node = atts.get(n);
      final QNm atn = node.qname();
      if(atn.ns() && !NSGlobal.standard(atn.uri().atom())) {
        ser.namespace(atn.pref(), atn.uri().atom());
      }
      ser.attribute(atn.atom(), node.atom());
    }

    // serialize children
    for(int n = 0; n < children.size(); ++n) children.get(n).serialize(ser);
    ser.closeElement();
  }

  @Override
  public FNode copy() {
    final FNode node = new FElem(name, ns, base).parent(par);
    for(int c = 0; c < children.size(); ++c) node.add(children.get(c).copy());
    for(int c = 0; c < atts.size(); ++c) node.add(atts.get(c).copy());
    return node;
  }

  @Override
  public void plan(final Serializer ser) throws IOException {
    ser.emptyElement(this, NAM, name.atom());
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("<");
    sb.append(Token.string(name.atom()));
    if(atts.size() != 0 || ns != null && ns.size != 0 || children.size() != 0)
      sb.append(" ...");
    return sb.append("/>").toString();
  }
}
