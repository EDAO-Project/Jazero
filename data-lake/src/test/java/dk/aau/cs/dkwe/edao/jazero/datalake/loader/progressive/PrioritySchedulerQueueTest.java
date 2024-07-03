package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrioritySchedulerQueueTest
{
    private PrioritySchedulerQueue queue;
    private final ItemIndexer<Table.Row<String>> indexer = (id, row, item) -> System.out.println();

    @BeforeEach
    public void setup()
    {
        Indexable i1 = new IndexTable(Path.of("path1.json"), 1, this.indexer),
                i2 = new IndexTable(Path.of("path2.json"), 1, this.indexer),
                i3 = new IndexTable(Path.of("path3.json"), 2, this.indexer);
        this.queue = new PrioritySchedulerQueue();
        this.queue.addIndexables(List.of(i1, i2, i3));
    }

    @Test
    public void testCountPriorities()
    {
        assertEquals(2, this.queue.countPriorities());
    }

    @Test
    public void testCountElements()
    {
        assertEquals(3, this.queue.countElements());
    }

    @Test
    public void testAddIndexable()
    {
        Indexable i4 = new IndexTable(Path.of("path4.json"), 3, this.indexer),
                i5 = new IndexTable(Path.of("path5.json"), 3, this.indexer),
                i6 = new IndexTable(Path.of("path6.json"), 4, this.indexer);
        this.queue.addIndexables(List.of(i4, i5, i6));
        assertEquals(4, this.queue.countPriorities());
        assertEquals(6, this.queue.countElements());
    }

    @Test
    public void testUpdate()
    {
        this.queue.update("path2", (indexable -> indexable.setPriority(3)));
        assertEquals(3, this.queue.countPriorities());
        assertEquals(3, this.queue.countElements());

        this.queue.update("path1", (indexable -> indexable.setPriority(3)));
        this.queue.update("path3", (indexable) -> indexable.setPriority(3));
        assertEquals(1, this.queue.countPriorities());
        assertEquals(3, this.queue.countElements());
    }

    @Test
    public void testPopIndexable()
    {
        Indexable popped = this.queue.popIndexable();
        assertEquals("path3", popped.getId());
    }
}
