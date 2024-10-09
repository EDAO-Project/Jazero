package dk.aau.cs.dkwe.edao.jazero.datalake.store.lsh;

import dk.aau.cs.dkwe.edao.jazero.datalake.store.Index;

import java.util.Set;

public interface LSHIndex<K, V> extends Index<K, V>
{
    Set<V> search(K key);
    Set<V> search(K key, int vote);
    Set<V> agggregatedSearch(K ... keys);
    Set<V> agggregatedSearch(int vote, K ... keys);
}
