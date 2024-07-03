package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import java.util.Collection;
import java.util.function.Consumer;

public interface SchedulerQueue
{
    void addIndexable(Indexable indexable);
    void addIndexables(Collection<Indexable> indexables);
    Indexable popIndexable();
    void update(String id, Consumer<Indexable> update);
}
