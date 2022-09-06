package dk.aau.cs.dkwe.edao.calypso.storagelayer.layer;

public interface Storage<E> extends Iterable<E>
{
    boolean insert(E element);
    int count();
}
