package org.basex.query.util.format;

import static org.basex.query.util.Err.*;
import static org.basex.util.Token.*;
import java.util.Calendar;
import java.util.HashMap;
import javax.xml.datatype.XMLGregorianCalendar;
import org.basex.query.QueryException;
import org.basex.query.item.AtomType;
import org.basex.query.item.Date;
import org.basex.util.InputInfo;
import org.basex.util.IntList;
import org.basex.util.Reflect;
import org.basex.util.TokenBuilder;
import org.basex.util.Util;

/**
 * Abstract class for formatting data in different languages.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Christian Gruen
 */
public abstract class Formatter extends FormatUtil {
  /** Language code: English. */
  private static final String EN = "en";

  /** Formatter instances. */
  private static final HashMap<String, Formatter> MAP =
    new HashMap<String, Formatter>();

  // initialize hash map with English formatter as default
  static { MAP.put(EN, new FormatterEN()); }

  /**
   * Returns a formatter for the specified language.
   * @param ln language
   * @return formatter instance
   */
  public static Formatter get(final String ln) {
    // check if formatter has already been created
    Formatter form = MAP.get(ln);
    if(form == null) {
      final String clz = Util.name(Formatter.class) + ln.toUpperCase();
      form = (Formatter) Reflect.get(Reflect.find(clz));
      // instantiation not successful: return default formatter
      if(form == null) form = MAP.get(EN);
    }
    return form;
  }

  /**
   * Returns a word representation for the specified number.
   * @param n number to be formatted
   * @param ord ordinal suffix
   * @return token
   */
  public abstract byte[] word(final long n, final byte[] ord);

  /**
   * Returns an ordinal representation for the specified number.
   * @param n number to be formatted
   * @param ord ordinal suffix
   * @return ordinal
   */
  public abstract byte[] ordinal(final long n, final byte[] ord);

  /**
   * Returns the specified month (0-11).
   * @param n number to be formatted
   * @param min minimum length
   * @param max maximum length
   * @return month
   */
  public abstract byte[] month(final int n, final int min, final int max);

  /**
   * Returns the specified day of the week (0-6, Sunday-Saturday).
   * @param n number to be formatted
   * @param min minimum length
   * @param max maximum length
   * @return day of week
   */
  public abstract byte[] day(final int n, final int min, final int max);

  /**
   * Returns the am/pm marker.
   * @param am am flag
   * @return am/pm marker
   */
  public abstract byte[] ampm(final boolean am);

  /**
   * Returns the calendar.
   * @return calendar
   */
  public abstract byte[] calendar();

  /**
   * Returns the era.
   * @param year year
   * @return era
   */
  public abstract byte[] era(final int year);

  /**
   * Formats the specified date.
   * @param date date to be formatted
   * @param pic picture
   * @param cal calendar
   * @param plc place
   * @param ii input info
   * @return formatted string
   * @throws QueryException query exception
   */
  public final byte[] formatDate(final Date date, final byte[] pic,
      final byte[] cal, final byte[] plc, final InputInfo ii)
      throws QueryException {

    // [CG] XQuery/Formatter: currently, calendars and places are ignored
    if(cal != null || plc != null);

    final TokenBuilder tb = new TokenBuilder();
    final DateParser dp = new DateParser(ii, pic);
    while(dp.more()) {
      final int ch = dp.next();
      if(ch != 0) {
        // print literal
        tb.add(ch);
      } else {
        byte[] m = dp.marker();
        if(m.length == 0) PICDATE.thrw(ii, pic);
        final int spec = ch(m, 0);
        m = substring(m, cl(m, 0));
        byte[] pres = ONE;
        long num = 0;

        final boolean dat = date.type == AtomType.DAT;
        final boolean tim = date.type == AtomType.TIM;
        final XMLGregorianCalendar gc = date.xc;
        boolean err = false;
        switch(spec) {
          case 'Y':
            num = gc.getYear();
            err = tim;
            break;
          case 'M':
            num = gc.getMonth();
            err = tim;
            break;
          case 'D':
            num = gc.getDay();
            err = tim;
            break;
          case 'd':
            num = Date.days(0, gc.getMonth(), gc.getDay());
            err = tim;
            break;
          case 'F':
            num = gc.toGregorianCalendar().get(Calendar.DAY_OF_WEEK) - 1;
            pres = new byte[] { 'n' };
            err = tim;
            break;
          case 'W':
            num = gc.toGregorianCalendar().get(Calendar.WEEK_OF_YEAR);
            err = tim;
            break;
          case 'w':
            num = gc.toGregorianCalendar().get(Calendar.WEEK_OF_MONTH);
            err = tim;
            break;
          case 'H':
            num = gc.getHour();
            err = dat;
            break;
          case 'h':
            num = gc.getHour() % 12;
            err = dat;
            break;
          case 'P':
            num = gc.getHour() / 12;
            pres = new byte[] { 'n' };
            err = dat;
            break;
          case 'm':
            num = gc.getMinute();
            pres = token("01");
            err = dat;
            break;
          case 's':
            num = gc.getSecond();
            pres = token("01");
            err = dat;
            break;
          case 'f':
            num = gc.getMillisecond();
            err = dat;
            break;
          case 'Z':
            num = gc.getTimezone();
            pres = token("01:01");
            break;
          case 'z':
            num = gc.getTimezone();
            pres = token("01:01");
            break;
          case 'C':
            pres = new byte[] { 'n' };
            break;
          case 'E':
            num = gc.getYear();
            pres = new byte[] { 'n' };
            break;
          default:
            err = true;
            break;
        }
        if(err) PICCOMP.thrw(ii, pic);

        final FormatParser fp = new FormatParser(ii, m, pres);
        if(fp.digit == 'n') {
          byte[] in = EMPTY;
          if(spec == 'M') {
            in = month((int) num - 1, fp.min, fp.max);
          } else if(spec == 'F') {
            in = day((int) num, fp.min, fp.max);
          } else if(spec == 'P') {
            in = ampm(num == 0);
          } else if(spec == 'C') {
            in = calendar();
          } else if(spec == 'E') {
            in = era((int) num);
          }
          if(fp.cs == Case.LOWER) in = lc(in);
          if(fp.cs == Case.UPPER) in = uc(in);
          tb.add(in);
        } else {
          tb.add(formatInt(num, fp));
        }
      }
    }
    return tb.finish();
  }

  /**
   * Returns a formatted integer.
   * @param num integer to be formatted
   * @param mp marker parser
   * @return string representation
   */
  public final byte[] formatInt(final long num, final FormatParser mp) {
    // choose sign
    long n = num;
    final boolean sign = n < 0;
    if(sign) n = -n;

    final TokenBuilder tb = new TokenBuilder();
    final int ch = mp.digit;
    final boolean single = mp.primary.length == cl(mp.primary, 0);

    if(ch == 'w') {
      tb.add(word(n, mp.ordinal));
    } else if(ch == KANJI[1]) {
      japanese(tb, n);
    } else if(single && ch == 'i') {
      roman(tb, n);
    } else if(ch >= '\u2460' && ch <= '\u249b') {
      if(num < 1 || num > 20) tb.addLong(num);
      else tb.add((int) (ch + num - 1));
    } else {
      final String seq = sequence(ch);
      if(seq != null) alpha(tb, num, seq);
      else tb.add(number(n, mp, zeroes(ch)));
    }

    // finalize formatted string
    byte[] in = tb.finish();
    if(mp.cs == Case.LOWER) in = lc(in);
    if(mp.cs == Case.UPPER) in = uc(in);
    return sign ? concat(new byte[] { '-' }, in) : in;
  }

  /**
   * Returns a character sequence based on the specified alphabet.
   * @param tb token builder
   * @param n number to be formatted
   * @param a alphabet
   */
  private static void alpha(final TokenBuilder tb, final long n,
      final String a) {

    final int al = a.length();
    if(n > al) alpha(tb, (n - 1) / al, a);
    tb.add(a.charAt((int) ((n - 1) % al)));
  }

  /**
   * Adds a Roman character sequence.
   * @param tb token builder
   * @param n number to be formatted
   */
  private static void roman(final TokenBuilder tb, final long n) {
    if(n > 0 && n < 4000) {
      final int v = (int) n;
      tb.add(ROMANM[v / 1000]);
      tb.add(ROMANC[v / 100 % 10]);
      tb.add(ROMANX[v / 10 % 10]);
      tb.add(ROMANI[v % 10]);
    } else {
      tb.addLong(n);
    }
  }

  /**
   * Adds a Japanese character sequence.
   * @param tb token builder
   * @param n number to be formatted
   */
  private void japanese(final TokenBuilder tb, final long n) {
    if(n == 0) {
      tb.add(KANJI[0]);
    } else {
      jp(tb, n, false);
    }
  }

  /**
   * Recursively adds a Japanese character sequence.
   * @param tb token builder
   * @param n number to be formatted
   * @param i initial call
   */
  private static void jp(final TokenBuilder tb, final long n, final boolean i) {
    if(n == 0) {
    } else if(n <= 9) {
      if(n != 1 || !i) tb.add(KANJI[(int) n]);
    } else if(n == 10) {
      tb.add(KANJI[10]);
    } else if(n <= 99) {
      jp(tb, n, 10, 10);
    } else if(n <= 999) {
      jp(tb, n, 100, 11);
    } else if(n <= 9999) {
      jp(tb, n, 1000, 12);
    } else if(n <= 99999999) {
      jp(tb, n, 10000, 13);
    } else if(n <= 999999999999L) {
      jp(tb, n, 100000000, 14);
    } else if(n <= 9999999999999999L) {
      jp(tb, n, 1000000000000L, 15);
    } else {
      tb.addLong(n);
    }
  }

  /**
   * Recursively adds a Japanese character sequence.
   * @param tb token builder
   * @param n number to be formatted
   * @param f factor
   * @param o kanji offset
   */
  private static void jp(final TokenBuilder tb, final long n, final long f,
      final int o) {
    jp(tb, n / f, true);
    tb.add(KANJI[o]);
    jp(tb, n % f, false);
  }

  /**
   * Creates a number character sequence.
   * @param n number to be formatted
   * @param mp marker parser
   * @param z zero digit
   * @return number character sequence
   */
  private byte[] number(final long n, final FormatParser mp, final int z) {
    // cache characters of presentation modifier
    final IntList pr = new IntList(mp.primary.length);
    for(int p = 0; p < mp.primary.length; p += cl(mp.primary, p)) {
      pr.add(cp(mp.primary, p));
    }

    // check for a regular separator pattern
    int rp = -1;
    boolean reg = false;
    for(int p = pr.size() - 1; p >= 0; --p) {
      final int ch = pr.get(p);
      if(ch == '#' || ch >= z && ch <= z + 9) continue;
      if(rp == -1) rp = pr.size() - p;
      reg = (pr.size() - p) % rp == 0;
    }
    final int rc = reg ? pr.get(pr.size() - rp) : 0;
    if(!reg) rp = Integer.MAX_VALUE;

    // build string representation in a reverse order
    final IntList cache = new IntList();
    final byte[] s = token(n);
    int b = s.length - 1, p = pr.size() - 1;
    while(p >= 0 && b >= 0) {
      final int ch = pr.get(p--);
      if(ch == '#' && cache.size() % rp == rp - 1) cache.add(rc);
      cache.add(ch == '#' || ch >= z && ch <= z + 9 ? s[b--] - '0' + z : ch);
    }
    // add remaining numbers
    while(b >= 0) {
      if(cache.size() % rp == rp - 1) cache.add(rc);
      cache.add(s[b--] - '0' + z);
    }
    // add remaining modifiers
    while(p >= 0) {
      final int ch = pr.get(p--);
      if(ch == '#') break;
      cache.add(ch >= z && ch <= z + 9 ? z : ch);
    }

    // reverse result and add ordinal suffix
    final TokenBuilder tb = new TokenBuilder();
    for(int c = cache.size() - 1; c >= 0; --c) tb.add(cache.get(c));
    return tb.add(ordinal(n, mp.ordinal)).finish();
  }
}
