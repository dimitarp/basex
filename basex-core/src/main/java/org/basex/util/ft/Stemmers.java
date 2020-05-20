package org.basex.util.ft;

import java.util.ArrayList;
import java.util.Collections;

/** Stemmers registry. */
public final class Stemmers {
  /** List of available stemmers. */
  static final ArrayList<Stemmer> IMPL = new ArrayList<>();

  /* Load stemmers and order them by precedence. */
  static {
    // built-in stemmers
    IMPL.add(new EnglishStemmer(null));
    IMPL.add(new GermanStemmer(null));
    IMPL.add(new GreekStemmer(null));
    IMPL.add(new IndonesianStemmer(null));
    IMPL.add(new DummyStemmer(null));

    if(SnowballStemmer.available()) IMPL.add(new SnowballStemmer());
    if(LuceneStemmer.available()) IMPL.add(new LuceneStemmer());
    if(WordnetStemmer.available()) IMPL.add(new WordnetStemmer());

    // sort stemmers and tokenizers by precedence
    Collections.sort(IMPL);
  }

  /**
   * Find a stemmer implementation for the given language.
   * @param language language to be found
   * @return {@link Stemmer} implementation
   */
  public static Stemmer getImpl(final Language language) {
    for(final Stemmer stem : IMPL) {
      if(stem.supports(language)) {
        return stem;
      }
    }
    // use default stemmer if specific stemmer is not available.
    return IMPL.get(0);
  }

  /**
   * Checks if the language is supported by the available stemmers.
   * @param language language to be found
   * @return result of check
   */
  public static boolean supportFor(final Language language) {
    for(final Stemmer impl : IMPL) {
      if(impl.supports(language)) return true;
    }
    return false;
  }

  /** Private constructor. */
  private Stemmers() {
  }
}