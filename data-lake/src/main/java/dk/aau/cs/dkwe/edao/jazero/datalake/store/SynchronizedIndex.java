package dk.aau.cs.dkwe.edao.jazero.datalake.store;

import java.io.Serializable;

public record SynchronizedIndex<K, V>(Index<K, V> index) implements Index<K, V>, Serializable
{
    public static <Key, Value> SynchronizedIndex<Key, Value> wrap(Index<Key, Value> index)
    {
        return new SynchronizedIndex<>(index);
    }

    @Override
    public synchronized void insert(K key, V value)
    {
        this.index.insert(key, value);
    }

    @Override
    public synchronized boolean remove(K key)
    {
        return this.index.remove(key);
    }

    @Override
    public synchronized V find(K key)
    {
        return this.index.find(key);
    }

    @Override
    public synchronized boolean contains(K key)
    {
        return this.index.contains(key);
    }

    @Override
    public int size()
    {
        return this.index.size();
    }

    @Override
    public void clear()
    {
        this.index.clear();
    }
}
