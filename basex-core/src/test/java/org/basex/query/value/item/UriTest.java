package org.basex.query.value.item;


import static org.junit.Assert.*;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.*;

/**
 * URI tests.
 *
 * @author BaseX Team 2005-15, BSD License
 * @author Dimitar Popov
 */
@RunWith(Parameterized.class)
public class UriTest {

  @Parameters(name = "{index}: \"{0}\": valid = {1}, absolute = {2}")
  public static Object[][] sampleUris() {
    return new Object[][] {
      {"x:", true, true},
      {"x", true, false},
      {"", true, false},
      {"//test.org:80", true, false},
      {"//[fe80::216:ceff:fe86:3e33]", true, false},
      {"x+y://a:b@[fe80::216:ceff:fe86:3e33]:80/p/b/c?q=1&q=2#test?123", true, true},
      {"x+y://a:b@254.254.254.254:80/p/b254/c?q=1&q=2#test?123", true, true},
      {"http://!$&amp;'()*+,;=/", true, true},
      {"http://%0Ad%E2%9C%90%F0%98%9A%A0/", true, true},
      {"odd-scheme+1.://www.example.org/", true, true},
      {"http://www.example.org/%0Ad%E2%9C%90%F0%98%9A%A0/", true, true},
      {"http://www.example.org/!/$/&/'/(/)/*/+/,/;/=/", true, true},
      {"http://www.example.org/:/@/", true, true},
      {"http://www.example.org/?%0Ad%E2%9C%90%F0%98%9A%A0/", true, true},
      {"http://www.example.org/?!$&'()*+,;=", true, true},
      {"http://www.example.org/?:@", true, true},
      {"http://www.example.org/#%0Ad%E2%9C%90%F0%98%9A%A0/", true, true},
      {"http://www.example.org/#!$&'()*+,;=", true, true},
      {"http://www.example.org/#:@", true, true},

      {"1:", false, false},
      {"invalidURI%gg", false, false},
      {"ttp:\\\\invalidURI\\someURI%gg", false, false},
      {":/", false, false},
      {"##invalid", false, false},
      {"%gg", false, false},
      {"%", false, false},
      {"http://www.example.com/file%GF.html", false, false},
      {":/cut.jpg", false, false},
      {":/images/cut.png", false, false},
      {"http:\\\\invalid%>URI\\someURI", false, false},
    };
  }

  @Parameter(0) public String uri;
  @Parameter(1) public boolean valid;
  @Parameter(2) public boolean absolute;

  /**
   * Tests for {@link Uri#isAbsolute()}.
   */
  @Test
  public void isAbsolute() {
    assertEquals("Uri absolute check failed", absolute, Uri.uri(uri).isAbsolute());
  }

  /**
   * Tests for {@link Uri#isValid()}.
   */
  @Test
  public void isValid() {
    assertEquals("Uri validation failed", valid, Uri.uri(uri).isValid());
  }
}
