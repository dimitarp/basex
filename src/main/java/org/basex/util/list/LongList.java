package org.basex.util.list;

import java.util.*;

import org.basex.util.*;

public class LongList extends ElementList {
  /** Element container. */
  protected long[] list;

  /**
   * Default constructor.
   */
  public LongList() {
    this(CAP);
  }

  /**
   * Constructor, specifying an initial array capacity.
   * @param c array capacity
   */
  public LongList(final int c) {
    list = new long[c];
  }

  /**
   * Constructor.
   * @param f resize factor
   */
  public LongList(final double f) {
    this();
    factor = f;
  }

  /**
   * Constructor, specifying an initial array.
   * @param a initial array
   */
  public LongList(final long[] a) {
    list = a;
    size = a.length;
  }

  /**
   * Adds an entry to the array.
   * @param e entry to be added
   */
  public final void add(final long e) {
    if(size == list.length) list = Arrays.copyOf(list, newSize());
    list[size++] = e;
  }

  /**
   * Returns the element at the specified index position.
   * @param i index
   * @return element
   */
  public final long get(final int i) {
    return list[i];
  }

  /**
   * Sets an element at the specified index position.
   * @param i index
   * @param e element to be set
   */
  public final void set(final int i, final long e) {
    if(i >= list.length) list = Arrays.copyOf(list, newSize(i + 1));
    list[i] = e;
    size = Math.max(size, i + 1);
  }

  /**
   * Checks if the specified element is found in the list.
   * @param e element to be found
   * @return result of check
   */
  public final boolean contains(final long e) {
    for(int i = 0; i < size; ++i) if(list[i] == e) return true;
    return false;
  }

  /**
   * Inserts elements at the specified index position.
   * @param i index
   * @param e elements to be inserted
   */
  public final void insert(final int i, final long[] e) {
    final int l = e.length;
    if(l == 0) return;
    if(size + l > list.length) list = Arrays.copyOf(list, newSize(size + l));
    Array.move(list, i, l, size - i);
    System.arraycopy(e, 0, list, i, l);
    size += l;
  }

  /**
   * Deletes the element at the specified position.
   * @param i position to delete
   */
  public final void delete(final int i) {
    Array.move(list, i + 1, -1, --size - i);
  }

  /**
   * Adds a difference to all elements starting from the specified index.
   * @param e difference
   * @param i index
   */
  public final void move(final long e, final int i) {
    for(int a = i; a < size; a++) list[a] += e;
  }

  /**
   * Returns the uppermost element from the stack.
   * @return the uppermost element
   */
  public final long peek() {
    return list[size - 1];
  }

  /**
   * Pops the uppermost element from the stack.
   * @return the popped element
   */
  public final long pop() {
    return list[--size];
  }

  /**
   * Pushes an element onto the stack.
   * @param val element
   */
  public final void push(final long val) {
    add(val);
  }

  /**
   * Searches the specified element via binary search.
   * Note that all elements must be sorted.
   * @param e element to be found
   * @return index of the search key, or the negative insertion point - 1
   */
  public final int sortedIndexOf(final int e) {
    return Arrays.binarySearch(list, 0, size, e);
  }

  /**
   * Returns an array with all elements.
   * @return array
   */
  public final long[] toArray() {
    return Arrays.copyOf(list, size);
  }

  /**
   * Sorts the data.
   * @return self reference
   */
  public LongList sort() {
    Arrays.sort(list, 0, size);
    return this;
  }

  @Override
  public String toString() {
    if(size == 0) return "";
    final StringBuilder tb = new StringBuilder(Util.name(this));
    tb.append('[');
    tb.append(list[0]);
    for(int i = 1; i < size; ++i) {
      tb.append(", ");
      tb.append(list[i]);
    }
    return tb.append(']').toString();
  }
}
