package dk.aau.cs.dkwe.edao.jazero.entitylinker.link;

public interface EntityLink<K, V>
{
    V link(K key);
}
