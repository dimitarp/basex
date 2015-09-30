package org.basex.query.value.item;

import static org.basex.query.QueryError.*;

import static org.basex.util.Strings.UTF8;

import java.io.ByteArrayInputStream;

import java.io.ByteArrayInputStream;
import java.net.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.basex.query.*;
import org.basex.query.value.type.*;
import org.basex.util.*;

/**
 * URI item ({@code xs:anyURI}).
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Christian Gruen
 */
public final class Uri extends AStr {
  /** Empty URI. */
  public static final Uri EMPTY = new Uri(Token.EMPTY);
  /** String data. */
  private final byte[] value;

  /**
   * Constructor.
   * @param value value
   */
  private Uri(final byte[] value) {
    super(AtomType.URI);
    this.value = value;
  }

  /**
   * Creates a new uri instance.
   * @param value value
   * @return uri instance
   */
  public static Uri uri(final byte[] value) {
    return uri(value, true);
  }

  /**
   * Creates a new uri instance.
   * @param value string value
   * @return uri instance
   */
  public static Uri uri(final String value) {
    return uri(Token.token(value), true);
  }

  /**
   * Creates a new uri instance.
   * @param value value
   * @param normalize chop leading and trailing whitespaces
   * @return uri instance
   */
  public static Uri uri(final byte[] value, final boolean normalize) {
    final byte[] u = normalize ? Token.normalize(value) : value;
    return u.length == 0 ? EMPTY : new Uri(u);
  }

  /**
   * Checks the URIs for equality.
   * @param uri to be compared
   * @return result of check
   */
  public boolean eq(final Uri uri) {
    return Token.eq(string(), uri.string());
  }

  /**
   * Appends the specified address. If one of the URIs is invalid,
   * the original uri is returned.
   * @param add address to be appended
   * @param info input info
   * @return new uri
   * @throws QueryException query exception
   */
  public Uri resolve(final Uri add, final InputInfo info) throws QueryException {
    if(add.value.length == 0) return this;
    try {
      final URI base = new URI(Token.string(value));
      final URI res = new URI(Token.string(add.value));
      final URI uri = base.resolve(res);
      return uri(Token.token(uri.toString()), false);
    } catch(final URISyntaxException ex) {
      throw URIARG_X.get(info, ex.getMessage());
    }
  }

  /**
   * Tests if this is an absolute URI.
   * @return result of check
   */
  public boolean isAbsolute() {
    return Token.contains(value, ':');
  }

  /**
   * Checks the validity of this URI.
   * @return result of check
   */
  public boolean isValid() {
    try {
      //new URI(Token.string(Token.uri(value, true)));
      final UriParser.ParsedUri parsed = UriParser.parse(Token.uri(value, true));
      return true;
    } catch(final Throwable ex) {
      ex.printStackTrace();
      return false;
    }
  }

  @Override
  public byte[] string(final InputInfo ii) {
    return value;
  }

  /**
   * Returns the string value.
   * @return string value
   */
  public byte[] string() {
    return value;
  }

  @Override
  public URI toJava() throws QueryException {
    try {
      return new URI(Token.string(value));
    } catch(final URISyntaxException ex) {
      throw new QueryException(ex);
    }
  }
}


/**
 * A parser for RFC 3986 URIs.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Dimitar Popov
 */
class UriParser {
  private static final String ALPHA = "A-Za-z";
  private static final String DIGIT = "0-9";
  private static final String HEXDIG = "[" + DIGIT + "A-F]";

  private static final String pctEncoded = "%" + HEXDIG + HEXDIG;

  private static final String unreserved = "[" + ALPHA + DIGIT + "._~-]";
  private static final String subDelims = "[!$&'()*+,;=]";
  private static final String genDelims = "[:/?#\\[\\]@]";

  private static final String  pchar = "(" + unreserved + "|" + pctEncoded + "|" + subDelims + "|:|@)";

  private static final String decOctet = "([0-9]|([1-9][0-9])|(1[0-9]{2})|(2[0-4][0-9])|(25[0-5]))";
  private static final String ipv4Address = decOctet + "\\." + decOctet + "\\." + decOctet + "\\." + decOctet;

  private static final String regName = "(" + unreserved + "|" + pctEncoded + "|" + subDelims + ")*";

  private static final String h16 = HEXDIG + "{1,4}";
  private static final String ls32 = "((" + h16 + ":" + h16 + ")|(" + ipv4Address + "))";

  private static final String ipv6Address = "("
  +  "("                               + "(" + h16 + ":){6}" + ls32 + ")"
  + "|("                             + "::(" + h16 + ":){5}" + ls32 + ")"
  + "|(("  +                   h16 + ")?::(" + h16 + ":){4}" + ls32 + ")"
  + "|(((" + h16 + ":){0,1}" + h16 + ")?::(" + h16 + ":){3}" + ls32 + ")"
  + "|(((" + h16 + ":){0,2}" + h16 + ")?::(" + h16 + ":){2}" + ls32 + ")"
  + "|(((" + h16 + ":){0,3}" + h16 + ")?::"  + h16 + ":"     + ls32 + ")"
  + "|(((" + h16 + ":){0,4}" + h16 + ")?::"                  + ls32 + ")"
  + "|(((" + h16 + ":){0,5}" + h16 + ")?::"                  + h16  + ")"
  + "|(((" + h16 + ":){0,6}" + h16 + ")?::"
  + ")";


  private static final String ipvFuture = "v" + HEXDIG + "+\\.(" + unreserved + "|" + subDelims + "|:)+";
  private static final String ipLiteral = "\\[(" + ipv6Address + "|" + ipvFuture + ")\\]";
  private static final String scheme = "(?<scheme>[" + ALPHA + "][" + ALPHA + DIGIT + "+.-]*)";
  private static final String userinfo = "(?<userinfo>(" + unreserved + "|" + pctEncoded + "|" + subDelims + "|:)*)";
  private static final String host = "(?<host>(" + ipLiteral + "|" + ipv4Address + "|" + regName + "))";
  private static final String port = "(?<port>" + DIGIT + "*)";
  private static final String authority = "(?<authority>(" + userinfo + "@)?" + host + "(:" + port + "))";
  private static final String path = "(?<path>[^?#]*)"; // TODO
  private static final String query = "(?<query>(" + pchar + "|/|\\?)*)";
  private static final String fragment = "(?<fragment>(" + pchar + "|/|\\?)*)";

  private static final Pattern rfc3986 = Pattern.compile(
    "^(" + scheme + ":)?(//" +  authority + ")?" + path + "(\\?" + query + ")?(#" + fragment + ")?");

  /**
   * Construct a new RFC 3986 URI parser.
   */
  private UriParser() {
  }

  /**
   * Parse an RFC 3986 URI.
   * @param uri the uri to parse
   * @return parsed URI
   */
  public static ParsedUri parse(final byte[] uri) throws URISyntaxException{
    final Matcher matcher = rfc3986.matcher(Token.string(uri));
    if (!matcher.matches()) throw new URISyntaxException("Invalid URI", "");
    return new ParsedUri.Builder()
      .scheme(matcher.group("scheme"))
      .authority(matcher.group("authority"))
      //.host(matcher.group())
      //.port()
      .path(matcher.group("path"))
      .query(matcher.group("query"))
      .fragment(matcher.group("fragment"))
      .build();
  }

  public static final class ParsedUri {
    public final String scheme;
    public final String authority;
    public final String userInfo;
    public final String host;
    public final int port;
    public final String path;
    public final String query;
    public final String fragment;

    private ParsedUri(String scheme, String authority, String userInfo, String host, int port, String path, String query, String fragment) {
      this.scheme = scheme;
      this.authority = authority;
      this.userInfo = userInfo;
      this.host = host;
      this.port = port;
      this.path = path;
      this.query = query;
      this.fragment = fragment;
    }

    @Override
    public String toString() {
      return "ParsedUri{" +
        "scheme='" + scheme + "'" +
        ", authority='" + authority + "'" +
        ", userInfo='" + userInfo + "'" +
        ", host='" + host + "'" +
        ", port=" + port +
        ", path='" + path + "'" +
        ", query='" + query + "'" +
        ", fragment='" + fragment + "'" +
        '}';
    }

    private static final class Builder {
      private String scheme;
      private String authority;
      private String userInfo;
      private String host;
      private int port = -1;
      private String path;
      private String query;
      private String fragment;

      Builder() {}

      public Builder scheme(String scheme) {
        this.scheme = scheme;
        return this;
      }

      public Builder authority(String authority) {
        this.authority = authority;
        return this;
      }

      public Builder userInfo(String userInfo) {
        this.userInfo = userInfo;
        return this;
      }

      public Builder host(String host) {
        this.host = host;
        return this;
      }

      public Builder port(int port) {
        this.port = port;
        return this;
      }

      public Builder path(String path) {
        this.path = path;
        return this;
      }

      public Builder query(String query) {
        this.query = query;
        return this;
      }

      public Builder fragment(String fragment) {
        this.fragment = fragment;
        return this;
      }

      public Builder but() {
        return new Builder()
          .authority(authority)
          .scheme(scheme)
          .host(host)
          .port(port)
          .path(path)
          .query(query)
          .fragment(fragment);
      }

      public ParsedUri build() {
        return new ParsedUri(
          scheme,
          authority,
          userInfo,
          host,
          port,
          path,
          query,
          fragment);
      }
    }
  }
}