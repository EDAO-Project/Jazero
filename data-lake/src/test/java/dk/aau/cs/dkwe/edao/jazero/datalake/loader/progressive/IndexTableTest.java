package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

public class IndexTableTest
{
    private IndexTable indexTable;
    private final File tableFile = new File("src/test/resources/tables/table-0832-36.json");

    @BeforeEach
    public void setup()
    {
        this.indexTable = new IndexTable(this.tableFile.toPath(), 10, (id, row, item) -> System.out.println());
    }

    @Test
    public void testGetPriority()
    {
        assertEquals(10, this.indexTable.getPriority());

        this.indexTable.increasePriority(15);
        assertEquals(25, this.indexTable.getPriority());

        this.indexTable.decreasePriority(10);
        assertEquals(15, this.indexTable.getPriority());
    }

    @Test
    public void testGetTable()
    {
        assertNull(this.indexTable.getTable());
        assertNotNull(this.indexTable.index());
        assertEquals(13, this.indexTable.getTable().rowCount());
    }

    @Test
    public void testGetId()
    {
        assertNotNull(this.indexTable.index());
        assertEquals("table-0832-36.json", this.indexTable.getId());
    }

    @Test
    public void testTablePath()
    {
        assertNotNull(this.indexTable.index());
        assertEquals(this.tableFile.toPath(), this.indexTable.getFilePath());
    }

    @Test
    public void testIsIndexed()
    {
        assertFalse(this.indexTable.isIndexed());
        assertNotNull(this.indexTable.index());
        assertFalse(this.indexTable.isIndexed());

        while (this.indexTable.getTable().rowCount() > 0)
        {
            assertNotNull(this.indexTable.index());
        }

        assertTrue(this.indexTable.isIndexed());
    }
}
