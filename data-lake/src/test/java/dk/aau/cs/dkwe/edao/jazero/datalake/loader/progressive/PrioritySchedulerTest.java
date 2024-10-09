package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class PrioritySchedulerTest
{
    private PriorityScheduler scheduler;
    private final ItemIndexer<Table.Row<String>> indexer = (id, row, item) -> System.out.println();
    private final IndexTable i1 = new IndexTable(Path.of("src/test/resources/tables/table-0832-36.json"), 1, this.indexer),
            i2 = new IndexTable(Path.of("src/test/resources/tables/table-0844-836.json"), 2, this.indexer),
            i3 = new IndexTable(Path.of("src/test/resources/tables/table-0869-972.json"), 3, this.indexer);

    @BeforeEach
    public void setup()
    {
        IndexTable i1 = new IndexTable(Path.of("src/test/resources/tables/table-0832-36.json"), 1, this.indexer),
                i2 = new IndexTable(Path.of("src/test/resources/tables/table-0844-836.json"), 2, this.indexer),
                i3 = new IndexTable(Path.of("src/test/resources/tables/table-0869-972.json"), 3, this.indexer);
        this.scheduler = new PriorityScheduler();
        this.scheduler.addIndexTables(List.of(i1, i2, i3));
    }

    @Test
    public void testHasNext()
    {
        assertTrue(this.scheduler.hasNext());

        this.scheduler.next();
        assertTrue(this.scheduler.hasNext());

        this.scheduler.next();
        assertTrue(this.scheduler.hasNext());

        this.scheduler.next();
        assertFalse(this.scheduler.hasNext());
    }

    @Test
    public void testNext()
    {
        Indexable next = this.scheduler.next();
        assertEquals(this.i3, next);

        next = this.scheduler.next();
        assertEquals(this.i2, next);

        next = this.scheduler.next();
        assertEquals(this.i1, next);
        assertNull(this.scheduler.next());
    }
}
