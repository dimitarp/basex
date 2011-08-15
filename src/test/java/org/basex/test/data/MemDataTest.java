package org.basex.test.data;

import org.basex.core.BaseXException;
import org.basex.core.Prop;
import org.basex.data.MemData;
import org.junit.Before;
/**
 * Test index updates when using memory storage ({@link MemData}).
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */

/**
 * Test index updates when using memory storage ({@link MemData}).
 *
 * @author BaseX Team 2005-11, BSD License
 * @author Dimitar Popov
 */
public final class MemDataTest extends DiskDataTest {
  @Override
  @Before
  public void setUp() throws BaseXException {
    ctx.prop.set(Prop.MAINMEM, true);
    super.setUp();
  }
}
