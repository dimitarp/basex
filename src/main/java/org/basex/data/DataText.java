package org.basex.data;

import static org.basex.util.Token.*;

/**
 * This class assembles texts which are used in the data classes.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public interface DataText {
  // META DATA ================================================================

  /** Database version; if it's modified, old database instances can't
   * be parsed anymore. */
  String STORAGE = "5.91";
  /** Index version; if it's modified, old indexes can't be parsed anymore. */
  String ISTORAGE = "5.91";

  /** Database version. */
  String DBSTR = "STORAGE";
  /** Database version. */
  String IDBSTR = "ISTORAGE";
  /** Last modification time. */
  String DBTIME = "TIME";
  /** Number of nodes. */
  String DBSIZE = "SIZE";
  /** File name. */
  String DBFNAME = "FNAME";
  /** File size. */
  String DBFSIZE = "FSIZE";
  /** Number of documents. */
  String DBNDOCS = "NDOCS";
  /** Encoding. */
  String DBENC = "ENCODING";
  /** Whitespace chopping. */
  String DBCHOP = "CHOPPED";
  /** Entity parsing. */
  String DBENTITY = "ENTITY";
  /** Path indexing. */
  String DBPTHIDX = "PTHINDEX";
  /** Text indexing. */
  String DBTXTIDX = "TXTINDEX";
  /** Attribute indexing. */
  String DBATVIDX = "ATVINDEX";
  /** Full-text indexing. */
  String DBFTXIDX = "FTXINDEX";
  /** Full-text wildcards indexing. */
  String DBWCIDX = "WCINDEX";
  /** Full-text stemming. */
  String DBFTST = "FTSTEM";
  /** Full-text language. */
  String DBFTLN = "FTLANG";
  /** Full-text case sensitivity. */
  String DBFTCS = "FTCS";
  /** Full-text diacritics removal. */
  String DBFTDC = "FTDC";
  /** Maximum scoring value. */
  String DBSCMAX = "FTSCMAX";
  /** Minimum scoring value. */
  String DBSCMIN = "FTSCMIN";
  /** Maximal indexed full-text score. */
  String DBSCTYPE = "FTSCTYPE";
  /** Up-to-date flag. */
  String DBUTD = "UPTODATE";
  /** Last (highest) id. */
  String DBLID = "LASTID";
  /** Permissions. */
  String DBPERM = "PERM";
  /** Documents. */
  String DBDOCS = "DOCS";

  /** Tags. */
  String DBTAGS = "TAGS";
  /** Attributes. */
  String DBATTS = "ATTS";
  /** Path summary. */
  String DBPATH = "PATH";
  /** Tags. */
  String DBNS = "NS";

  // XML SERIALIZATION ========================================================

  /** Yes flag. */
  String YES = "yes";
  /** No flag. */
  String NO = "no";
  /** Omit flag. */
  String OMIT = "omit";
  /** Version. */
  String V10 = "1.0";
  /** Version. */
  String V11 = "1.1";
  /** Version. */
  String V40 = "4.0";
  /** Version. */
  String V401 = "4.01";
  /** Method. */
  String M_XML = "xml";
  /** Method. */
  String M_XHTML = "xhtml";
  /** Method. */
  String M_HTML = "html";
  /** Method. */
  String M_CSV = "csv";
  /** Method. */
  String M_MAB2 = "mab2";
  /** Method. */
  String M_TEXT = "text";
  /** Normalization. */
  String NFC = "NFC";
  /** Normalization. */
  String NONE = "none";

  /** Ampersand entity. */
  byte[] E_AMP = token("&amp;");
  /** Quote entity. */
  byte[] E_QU = token("&quot;");
  /** GreaterThan entity. */
  byte[] E_GT = token("&gt;");
  /** LessThan entity. */
  byte[] E_LT = token("&lt;");
  /** HTML: Non-breaking space entity. */
  byte[] E_NBSP = token("&nbsp;");

  /** Results tag. */
  byte[] RESULTS = token("results");
  /** Result tag. */
  byte[] RESULT = token("result");
  /** Path tag. */
  byte[] PATH = token("path");
  /** Name tag. */
  byte[] NAME = token("name");
  /** Node tag. */
  byte[] NODE = token("node");
  /** Kind attribute. */
  byte[] KIND = token("kind");
  /** Size tag. */
  byte[] SIZE = token("size");

  /** Document declaration. */
  String DOCDECL1 = "xml version=\"";
  /** Document declaration. */
  String DOCDECL2 = "\" encoding=\"";
  /** Document declaration. */
  String DOCDECL3 = "\" standalone=\"";
  /** Doctype output. */
  String DOCTYPE = "<!DOCTYPE ";
  /** Doctype system keyword. */
  String SYSTEM = "SYSTEM";
  /** Doctype public keyword. */
  String PUBLIC = "PUBLIC";

  /** Comment output. */
  byte[] COM1 = token("<!--");
  /** Comment output. */
  byte[] COM2 = token("-->");
  /** PI output. */
  byte[] PI1 = token("<?");
  /** PI output. */
  byte[] PI2 = token("?>");

  /** Element output. */
  byte[] ELEM1 = { '<' };
  /** Element output. */
  byte[] ELEM2 = { '>' };
  /** Element output. */
  byte[] ELEM3 = token("</");
  /** Element output. */
  byte[] ELEM4 = token("/>");
  /** Attribute output. */
  byte[] ATT1 = token("=\"");
  /** Attribute output. */
  byte[] ATT2 = token("\"");
  /** Document output. */
  byte[] DOC = token("doc()");
  /** Text output. */
  byte[] TEXT = token("text()");
  /** Comment output. */
  byte[] COMM = token("comment()");
  /** Processing instruction output. */
  byte[] PI = token("processing-instruction()");
  /** Attribute output. */
  byte[] ATT = { '@' };
  /** CDATA output. */
  byte[] CDATA1 = token("<![CDATA[");
  /** CDATA output. */
  byte[] CDATA2 = token("]]>");

  /** HTML: head element. */
  byte[] HEAD = token("head");
  /** HTML: meta element. */
  byte[] META = token("meta");
  /** HTML: http-equiv attribute. */
  byte[] HTTPEQUIV = token("http-equiv");
  /** HTML: Content-Type attribute value. */
  byte[] CONTTYPE = token("Content-Type");
  /** HTML: content attribute. */
  byte[] CONTENT = token("content");
  /** HTML: charset attribute value. */
  byte[] CHARSET = token("; charset=");

  /** Serialization error. */
  String SERVAL = "Parameter '%' must be [%";
  /** Serialization error. */
  String SERVAL2 = "|%";
  /** Serialization error. */
  String SERVAL3 = "]; '%' found";

  // TABLE SERIALIZATION ======================================================

  /** First table Header. */
  byte[] TABLEID = token("ID");
  /** First table Header. */
  byte[] TABLEPRE = token("PRE");
  /** Second table Header. */
  byte[] TABLEDIST = token("DIS");
  /** Third table Header. */
  byte[] TABLESIZE = token("SIZ");
  /** Fourth table Header. */
  byte[] TABLEATS = token("ATS");
  /** Fifth table Header. */
  byte[] TABLEKND = token("KIND");
  /** Sixth table Header. */
  byte[] TABLECON = token("CONTENT");

  /** Namespace header. */
  byte[] TABLENS = token("NS");
  /** Prefix header. */
  byte[] TABLEPREF = token("PREF");
  /** URI header. */
  byte[] TABLEURI = token("URI");
  /** Table kinds. */
  byte[][] TABLEKINDS = tokens("DOC ", "ELEM", "TEXT", "ATTR", "COMM", "PI  ");

  // DATABASE FILES ===========================================================

  /** Database - Info. */
  String DATAINFO = "inf";
  /** Database - Tokens. */
  String DATATBL = "tbl";
  /** Database - Temporary Size References. */
  String DATATMP = "tmp";
  /** Database - Text index. */
  String DATATXT = "txt";
  /** Database - Attribute value index. */
  String DATAATV = "atv";
  /** Database - Full-text index. */
  String DATAFTX = "ftx";
  /** Database - Stopword list. */
  String DATASWL = "swl";
}
