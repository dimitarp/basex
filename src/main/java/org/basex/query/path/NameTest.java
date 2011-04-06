package org.basex.query.path;

import static org.basex.util.Token.*;
import static org.basex.query.QueryText.*;
import org.basex.data.Data;
import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.item.ANode;
import org.basex.query.item.NodeType;
import org.basex.query.item.QNm;
import org.basex.query.item.Uri;
import org.basex.util.InputInfo;

/**
 * Name test.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class NameTest extends Test {
  /** Input information. */
  public final InputInfo input;
  /** Local name. */
  public final byte[] ln;

  /**
   * Empty constructor ('*').
   * @param att attribute flag
   * @param ii input info
   */
  public NameTest(final boolean att, final InputInfo ii) {
    this(null, Name.ALL, att, ii);
  }

  /**
   * Constructor.
   * @param nm name
   * @param t type of name test
   * @param att attribute flag
   * @param ii input info
   */
  public NameTest(final QNm nm, final Name t, final boolean att,
      final InputInfo ii) {
    type = att ? NodeType.ATT : NodeType.ELM;
    ln = nm != null ? nm.ln() : null;
    name = nm;
    test = t;
    input = ii;
  }

  @Override
  public boolean comp(final QueryContext ctx) throws QueryException {
    // check namespace context
    if(ctx.ns.size() != 0) {
      if(name != null && !name.hasUri()) {
        name.uri(ctx.ns.uri(name.pref(), false, input));
      }
    }

    // retrieve current data reference
    final Data data = ctx.data();
    if(data == null) return true;

    // skip optimizations if several namespaces are defined in the database
    final byte[] ns = data.ns.globalNS();
    if(ns == null) return true;

    // true if results can be expected
    boolean ok = true;

    if(test == Name.STD && !name.ns()) {
      // no results if default and database namespaces of elements are different
      ok = type == NodeType.ATT || eq(ns, ctx.nsElem);
      if(ok) {
        // identical namespace: ignore prefix to speed up test
        if(ns.length != 0) ctx.compInfo(OPTPREF, ln);
        test = Name.NAME;
      }
    }

    // check existence of tag/attribute names
    ok = ok && (test != Name.NAME || (type == NodeType.ELM ?
        data.tags : data.atts).id(ln) != 0);

    if(!ok) ctx.compInfo(OPTNAME, name);
    return ok;
  }

  @Override
  public boolean eval(final ANode node) {
    // only elements and attributes will yield results
    if(node.type != type) return false;

    switch(test) {
      // wildcard - accept all nodes
      case ALL:
        return true;
      // namespaces wildcard - check only name
      case NAME:
        return eq(ln, ln(node.nname()));
      // name wildcard - check only namespace
      case NS:
        return name.uri().eq(node.qname(tmpq).uri());
      default:
        // check attributes, or check everything
        return type == NodeType.ATT && !name.ns() ? eq(ln, node.nname()) :
          name.eq(node.qname(tmpq));
    }
  }

  @Override
  public String toString() {
    if(test == Name.ALL) return "*";
    if(test == Name.NAME) return "*:" + string(name.atom());
    final String uri = name.uri() == Uri.EMPTY || name.ns() ? "" :
      "{" + string(name.uri().atom()) + "}";
    return uri + (test == Name.NS ? "*" : string(name.atom()));
  }
}
