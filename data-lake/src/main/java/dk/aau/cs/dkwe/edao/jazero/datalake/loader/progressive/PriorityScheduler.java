package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import java.util.Collection;
import java.util.function.Consumer;

/**
 * Scheduler that select the table of the highest priority to schedule
 */
public class PriorityScheduler implements Scheduler
{
    private final PrioritySchedulerQueue queue = new PrioritySchedulerQueue();

    @Override
    public void addIndexTable(Indexable indexTable)
    {
        this.queue.addIndexable(indexTable);
    }

    @Override
    public void addIndexTables(Collection<Indexable> indexTables)
    {
        this.queue.addIndexables(indexTables);
    }

    @Override
    public boolean hasNext()
    {
        return this.queue.countElements() > 0;
    }

    @Override
    public Indexable next()
    {
        if (!hasNext())
        {
            return null;
        }

        return this.queue.popIndexable();
    }

    @Override
    public void update(String id, Consumer<Indexable> update)
    {
        this.queue.update(id, update);
    }
}
