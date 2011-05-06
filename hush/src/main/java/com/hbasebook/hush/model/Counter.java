package com.hbasebook.hush.model;

/**
 * Helper to sort counters by key or value.
 * 
 * @param <K> The key type.
 * @param <V> The value type.
 */
public class Counter<K extends Comparable<K>, V extends Comparable<V>>
    implements Comparable<Counter<K, V>> {

  public enum Sort {
    KeyAsc, KeyDesc, ValueAsc, ValueDesc;
  }

  private K key;
  private V value;
  private Sort sort;

  public Counter(K key, V value) {
    this(key, value, Sort.KeyAsc);
  }

  public Counter(K key, V value, Sort sort) {
    this.key = key;
    this.value = value;
    this.sort = sort;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V value) {
    this.value = value;
  }

  public Sort getSort() {
    return sort;
  }

  public void setSort(Sort sort) {
    this.sort = sort;
  }

  @Override
  public int compareTo(Counter<K, V> other) {
    switch (sort) {
    case KeyDesc:
      return other.key.compareTo(key);
    case ValueAsc:
      return value.compareTo(other.value);
    case ValueDesc:
      return other.value.compareTo(value);
    default:
      return key.compareTo(other.key);
    }
  }
}
