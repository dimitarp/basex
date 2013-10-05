package org.basex.build;

import static org.basex.query.util.Err.*;

import java.io.*;
import java.util.*;

import org.basex.core.*;
import org.basex.io.serial.*;
import org.basex.util.*;

/**
 * This class contains parser properties.
 *
 * @author BaseX Team 2005-12, BSD License
 * @author Christian Gruen
 */
public final class CsvProp extends AProp {
  /** Option: encoding. */
  public static final Object[] ENCODING = { "encoding", Token.UTF8 };
  /** Option: column separator. */
  public static final Object[] SEPARATOR = { "separator", "comma" };
  /** Option: header line. */
  public static final Object[] HEADER = { "header", false };
  /** Option: lax conversion of strings to QNames. */
  public static final Object[] LAX = { "lax", true };

  /** CSV separators. */
  public static enum CsvSep {
    /** Comma.     */ COMMA(','),
    /** Semicolon. */ SEMICOLON(';'),
    /** Colon.     */ COLON(':'),
    /** Tab.       */ TAB('\t'),
    /** Space.     */ SPACE(' ');

    /** Character. */
    private final int ch;

    /**
     * Constructor.
     * @param c mapped character
     */
    private CsvSep(final int c) {
      ch = c;
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase(Locale.ENGLISH);
    }
  }

  /**
   * Constructor.
   */
  public CsvProp() {
    super();
  }

  /**
   * Constructor, specifying initial properties.
   * @param s property string
   * @throws IOException I/O exception
   */
  public CsvProp(final String s) throws IOException {
    parse(s);
  }

  /**
   * Returns the separator character.
   * @return separator
   * @throws SerializerException serializer exception
   */
  public int separator() throws SerializerException {
    // set separator
    final String sep = get(SEPARATOR);
    final String val = sep.toLowerCase(Locale.ENGLISH);
    for(final CsvSep s : CsvSep.values()) {
      if(val.equals(s.toString())) return s.ch;
    }
    final byte[] s = Token.token(sep);
    final int sl = s.length;
    if(sl > 0 && Token.cl(s, 0) == sl) return Token.cp(s, 0);
    throw BXCS_CONFIG.thrwSerial(sep);
  }
}
