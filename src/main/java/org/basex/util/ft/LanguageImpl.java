package org.basex.util.ft;

import java.util.EnumSet;

/**
 * Functions for judging which classes (eg. tokenizers, stemmers) match to
 * chosen language.
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Jens Erat
 */
abstract class LanguageImpl extends FTIterator
    implements Comparable<LanguageImpl> {

  /**
   * Returns the precedence of the processor.
   * @return precedence
   */
  abstract int prec();

  /**
   * Checks if the specified language is supported.
   * @param ln language
   * @return true if language is supported
   */
  public boolean supports(final Language ln) {
    return languages().contains(ln);
  }

  /**
   * Returns the supported languages.
   * @return languages
   */
  abstract EnumSet<Language> languages();

  @Override
  public final boolean equals(final Object o) {
    return o instanceof LanguageImpl && ((LanguageImpl) o).prec() == prec();
  }

  @Override
  public final int compareTo(final LanguageImpl o) {
    // higher precedence value = better
    return o.prec() - prec();
  }

  @Override
  public int hashCode() {
    return prec();
  }
}
