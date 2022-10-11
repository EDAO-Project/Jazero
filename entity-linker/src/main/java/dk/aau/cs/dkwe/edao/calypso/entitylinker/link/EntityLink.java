package dk.aau.cs.dkwe.edao.calypso.entitylinker.link;

public interface EntityLink<K, V>
{
    V link(K key);
}
