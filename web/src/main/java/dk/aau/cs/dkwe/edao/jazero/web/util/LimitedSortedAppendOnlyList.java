package dk.aau.cs.dkwe.edao.jazero.web.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Stream;

public class LimitedSortedAppendOnlyList<T>
{
    private final int limit;
    private final TreeSet<T> items;

    public LimitedSortedAppendOnlyList(int limit, Comparator<T> comparator)
    {
        this.limit = limit;
        this.items = new TreeSet<>(comparator);
    }

    public void add(T item)
    {
        this.items.add(item);

        if (this.items.size() > this.limit)
        {
            items.pollFirst();
        }
    }

    public void addAll(Collection<T> items)
    {
        items.forEach(this::add);
    }

    public Stream<T> stream()
    {
        return this.items.stream();
    }

    public Optional<T> getLast()
    {
        if (this.items.isEmpty())
        {
            return Optional.empty();
        }

        return Optional.of(this.items.last());
    }
}
