package org.basex.io.random.file;

import static org.junit.Assert.*;

import org.basex.io.random.file.Buffer;
import org.junit.Test;

public abstract class BufferTest {
  private static final int SIZE = 10;

  Buffer sut = getBuffer(SIZE);

  protected abstract Buffer getBuffer(int size);

  @Test
  public void testGetAndSet_first() throws Exception {
    int index = 0;
    byte b = 123;
    sut.set(index, b);
    assertEquals(sut.get(index), b);
  }

  @Test
  public void testGetAndSet_last() throws Exception {
    int index = SIZE - 1;
    byte b = 123;
    sut.set(index, b);
    assertEquals(sut.get(index), b);
  }

  @Test
  public void testGetAndSetPos() throws Exception {
    long pos = Long.MAX_VALUE;
    sut.setPos(pos);
    assertEquals(sut.getPos(), pos);
  }

  @Test
  public void testIsAndSetDirty_true() throws Exception {
    sut.setDirty(true);
    assertTrue(sut.isDirty());
  }

  @Test
  public void testIsAndSetDirty_false() throws Exception {
    sut.setDirty(false);
    assertFalse(sut.isDirty());
  }

  @Test
  public void testCopyToAndFrom() throws Exception {
    int length = 5;
    int pos = SIZE - length;
    int off = 3;
    byte[] src = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    byte[] dst = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    sut.copyFrom(pos, src, off, length);
    sut.copyTo(pos, dst, off, length);

    assertArrayEquals(dst, new byte[] {0, 0, 0, 3, 4, 5, 6, 7, 0, 0});
  }

  @Test
  public void testMove() throws Exception {
    byte[] src = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    byte[] dst = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};

    sut.copyFrom(0, src, 0, src.length);
    sut.move(4, -3, 4);
    sut.copyTo(0, dst, 0, dst.length);

    assertArrayEquals(dst, new byte[] {0, 4, 5, 6, 7, 5, 6, 7, 8, 9});
  }

  @Test
  public void testRead1() throws Exception {
    sut.copyFrom(0, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0, 10);
    assertEquals(sut.read1(5), 5);
  }

  @Test
  public void testRead2() throws Exception {
    sut.copyFrom(0, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0, 10);
    assertEquals(sut.read2(5), (5 << 8) + 6);
  }

  @Test
  public void testRead4() throws Exception {
    sut.copyFrom(0, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0, 10);
    assertEquals(sut.read4(5), (5 << 24) + (6 << 16) + (7 << 8) + 8);
  }

  @Test
  public void testRead5() throws Exception {
    sut.copyFrom(0, new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0, 10);
    assertEquals(sut.read5(5), (5L << 32) + (6L << 24) + (7L << 16) + (8L << 8) + 9L);
  }

  @Test
  public void testWrite1() throws Exception {
    sut.write1(5, 5);

    byte[] actual = new byte[10];
    sut.copyTo(0, actual, 0, 10);
    assertArrayEquals(actual, new byte[] {0, 0, 0, 0, 0, 5, 0, 0, 0, 0});
  }

  @Test
  public void testWrite2() throws Exception {
    sut.write2(5, (5 << 8) + 6);

    byte[] actual = new byte[10];
    sut.copyTo(0, actual, 0, 10);
    assertArrayEquals(actual, new byte[] {0, 0, 0, 0, 0, 5, 6, 0, 0, 0});
  }

  @Test
  public void testWrite4() throws Exception {
    sut.write4(5, (5 << 24) + (6 << 16) + (7 << 8) + 8);

    byte[] actual = new byte[10];
    sut.copyTo(0, actual, 0, 10);
    assertArrayEquals(actual, new byte[] {0, 0, 0, 0, 0, 5, 6, 7, 8, 0});
  }

  @Test
  public void testWrite5() throws Exception {
    sut.write5(5, (5L << 32) + (6L << 24) + (7L << 16) + (8L << 8) + 9L);

    byte[] actual = new byte[10];
    sut.copyTo(0, actual, 0, 10);
    assertArrayEquals(actual, new byte[] {0, 0, 0, 0, 0, 5, 6, 7, 8, 9});
  }
}
