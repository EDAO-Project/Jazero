package dk.aau.cs.dkwe.edao.calypso.storagelayer.layer;

import java.io.File;
import java.util.Set;
import java.util.function.Predicate;

public interface Storage<E> extends Iterable<E>
{
    boolean insert(E element);
    int count();
    Set<E> elements();
    Set<E> elements(Predicate<E> predicate);
}
