package org.basex.util.ft;

import org.basex.util.*;

/**
 * Implementation of common stemmer methods.
 *
 * @author BaseX Team 2005-20, BSD License
 * @author Dimitar Popov
 */
public abstract class Stemmer extends LanguageImpl {
  /** Full-text iterator. */
  private final FTIterator iter;

  /**
   * Constructor.
   */
  Stemmer() {
    this(null);
  }

  /**
   * Constructor.
   * @param iter full-text iterator
   */
  Stemmer(final FTIterator iter) {
    this.iter = iter;
  }

  /**
   * Factory method.
   * @param lang language
   * @param fti full-text iterator
   * @return stemmer
   */
  abstract Stemmer get(Language lang, FTIterator fti);

  /**
   * Stems a word.
   * @param word input word to stem
   * @return the stem of the word
   */
  protected abstract byte[] stem(byte[] word);

  @Override
  public final Stemmer init(final byte[] txt) {
    iter.init(txt);
    return this;
  }

  @Override
  public final boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public final FTSpan next() {
    final FTSpan s = iter.next();
    s.text = stem(s.text);
    return s;
  }

  @Override
  public final byte[] nextToken() {
    return stem(iter.nextToken());
  }

  @Override
  public String toString() {
    return Util.className(this).replace("Stemmer", "");
  }
}
