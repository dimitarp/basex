package org.basex.io.random;

import static org.junit.Assert.*;
import static org.basex.util.Reflect.*;
import static org.basex.io.random.Blocks.*;
import static org.basex.io.random.RecordAccess.*;
import static org.mockito.MockitoAnnotations.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.basex.util.Reflect;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Suite.SuiteClasses;
import org.mockito.InOrder;
import org.mockito.Mock;

@SuiteClasses({ RecordAccessTest.BlocksTest.class, RecordAccessTest.class})
public class RecordAccessTest {

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public final void testFlush() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testClose() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testReadMetaData() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testWriteMetaData() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testRecordAccess() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testSelect() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testDelete() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testInsert() {
    fail("Not yet implemented"); // TODO
  }

  @Test
  public final void testBlock() throws Exception {
    assertEquals(0x7FFFFFFF,
        invoke(
            method(RecordAccess.class, "block", long.class),
            0x0000007FFFFFFF000L));
  }

  @Test
  public final void testSlot() throws Exception {
    assertEquals(0xFAC,
        invoke(
            method(RecordAccess.class, "slot", long.class),
            0x000000FFFFFFFFACL));
  }

  @Test
  public final void testHeaderIndex() throws Exception {
    final int expected = 23;
    assertEquals(expected,
        invoke(
            method(HeaderBlocks.class, "headerIndex", int.class),
            348 + expected * field(HeaderBlocks.class, "BLOCKS").getInt(null)));
  }


  static Object invoke(final Method m, final Object... args)
      throws Exception {
    return Reflect.invoke(m, (Object) null, args);
  }

  static Field field(final Class<?> cls, final String name)
      throws Exception {
    final Field f = cls.getDeclaredField(name);
    if(!f.isAccessible()) f.setAccessible(true);
    return f;
  }

  public static class BlocksTest {

    @Test
    public final void testFlush() throws Exception {

      final BlockManagedDataAccess da = mock(BlockManagedDataAccess.class);
      final Blocks blocks = mock(Blocks.class);
      field(Blocks.class, "da").set(blocks, da);
      doCallRealMethod().when(blocks).flush();

      blocks.flush();

      assertFalse(blocks.dirty);

      verify(blocks).writeMetaData();
      verify(da).flush();
    }

    @Test
    public final void testClose() throws Exception {

      final BlockManagedDataAccess da = mock(BlockManagedDataAccess.class);
      final Blocks blocks = mock(Blocks.class);
      field(Blocks.class, "da").set(blocks, da);
      doCallRealMethod().when(blocks).flush();
      doCallRealMethod().when(blocks).close();
      blocks.dirty = true;

      blocks.close();

      assertFalse(blocks.dirty);

      verify(blocks).writeMetaData();
      verify(blocks).flush();
      verify(da).flush();
    }

    @Test
    public final void testGotoBlock() {
      final long blockAddress = 10L;

      final Blocks blocks = mock(Blocks.class);
      doCallRealMethod().when(blocks).gotoBlock(blockAddress);

      blocks.gotoBlock(blockAddress);

      assertEquals(blockAddress, blocks.addr);
      assertFalse(blocks.dirty);

      verify(blocks).readMetaData();
      verify(blocks, never()).writeMetaData();
    }

    @Test
    public final void testGotoBlockSame() {
      final long blockAddress = 10L;

      final Blocks blocks = mock(Blocks.class);
      doCallRealMethod().when(blocks).gotoBlock(blockAddress);
      blocks.addr = blockAddress;
      blocks.dirty = true;

      blocks.gotoBlock(blockAddress);

      assertEquals(blockAddress, blocks.addr);
      assertTrue(blocks.dirty);

      verify(blocks, never()).readMetaData();
      verify(blocks, never()).writeMetaData();
    }

    @Test
    public final void testGotoBlockDirty() {
      final long blockAddress = 10L;

      final Blocks blocks = mock(Blocks.class);
      final InOrder inOrder = inOrder(blocks);
      doCallRealMethod().when(blocks).gotoBlock(blockAddress);
      blocks.dirty = true;

      blocks.gotoBlock(blockAddress);

      assertEquals(blockAddress, blocks.addr);
      assertFalse(blocks.dirty);

      inOrder.verify(blocks).writeMetaData();
      inOrder.verify(blocks).readMetaData();
    }

    @Test
    public final void testReadFirst12Bits() {
      assertEquals(0xFFF, readFirst12Bits((byte) 0xFF, (byte) 0x0F));
    }

    @Test
    public final void testReadLast12Bits() {
      assertEquals(0xFFF, readLast12Bits((byte) 0xF0, (byte) 0xFF));
    }

    @Test
    public final void testWriteFirst12Bits() {
      final byte[] data = new byte[2];
      writeFirst12Bits(data, 0, 1, 0xFFF);
      assertArrayEquals(new byte[] { (byte) 0xFF, (byte) 0x0F }, data);
    }

    @Test
    public final void testWriteLast12Bits() {
      final byte[] data = new byte[2];
      writeLast12Bits(data, 0, 1, 0xFFF);
      assertArrayEquals(new byte[] { (byte) 0xF0, (byte) 0xFF }, data);
    }
  }
}
