package org.basex.util;

/**
 * This class parses command-line arguments.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public final class Args {
  /** Calling object. */
  private final Object obj;
  /** Program header. */
  private final String header;
  /** Usage info. */
  private final String usage;
  /** Command-line arguments. */
  private final String args;

  /** OK flag. */
  private boolean ok = true;
  /** Dash flag. */
  private boolean dash;
  /** Current argument. */
  private int c;

  /**
   * Default constructor.
   * @param a arguments
   * @param o calling object
   * @param u usage info
   * @param h header
   */
  public Args(final String[] a, final Object o, final String u,
      final String h) {
    final StringBuilder sb = new StringBuilder();
    for(final String s : a) sb.append(s).append(' ');
    args = sb.toString();
    usage = u;
    obj = o;
    header = h;
  }

  /**
   * Simplified constructor.
   * @param a arguments
   * @param o calling object
   * @param u usage info
   */
  public Args(final String[] a, final Object o, final String u) {
    this(a, o, u, null);
  }

  /**
   * Checks if more arguments are available.
   * @return result of check
   */
  public boolean more() {
    while(ok && c < args.length()) {
      final char ch = args.charAt(c);
      if(ch == ' ') {
        if(dash) dash = false;
      } else if(!dash && ch == '-') {
        dash = true;
      } else {
        return true;
      }
      ++c;
    }
    return false;
  }

  /**
   * Checks if the current argument starts with a dash.
   * @return result of check
   */
  public boolean dash() {
    return dash;
  }

  /**
   * Returns the next character.
   * @return next character
   */
  public char next() {
    return args.charAt(c++);
  }

  /**
   * Returns the next string.
   * @return string
   */
  public String string() {
    while(args.charAt(c) == ' ') if(++c == args.length()) return "";
    final int i = args.indexOf(' ', c);
    final String s = args.substring(c, i);
    c = i;
    return s.trim();
  }

  /**
   * Returns the next number.
   * @return number as int value
   */
  public int num() {
    final int i = Token.toInt(string());
    ok = i != Integer.MIN_VALUE;
    return i;
  }

  /**
   * Returns the remaining string.
   * @return remaining string
   */
  public String remaining() {
    while(args.charAt(c) == ' ') if(++c == args.length()) return "";
    final String s = args.substring(c);
    c = args.length();
    return s.trim();
  }

  /**
   * Combines the specified boolean with the {@link #ok} flag with the AND
   * operator, and returns the result.
   * @param o ok flag to be combined
   * @return resulting ok flag
   */
  public boolean ok(final boolean o) {
    ok &= o;
    return ok;
  }

  /**
   * Return success flag; if false, print usage message.
   * @return success flag
   */
  public boolean finish() {
    if(!ok) Util.outln((header != null ? header : "") +
        "Usage: " + Util.name(obj).toLowerCase() + usage);
    return ok;
  }

  @Override
  public String toString() {
    return args;
  }
}
