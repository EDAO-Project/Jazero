package dk.aau.cs.dkwe.edao.calypso.datalake.structures.table;

import java.util.Iterator;
import java.util.List;

public class MultiIterator<E> implements Iterator<E>
{
    private List<Iterator<E>> iterators;
    private int pointer = 0;

    public MultiIterator(List<Iterator<E>> iterators)
    {
        this.iterators = iterators;
    }

    @Override
    public boolean hasNext()
    {
        if (this.pointer >= this.iterators.size())
        {
            return false;
        }

        return this.iterators.get(this.pointer).hasNext();
    }

    @Override
    public E next()
    {
        if (this.pointer >= this.iterators.size())
        {
            return null;
        }

        return this.iterators.get(this.pointer++).next();
    }
}
