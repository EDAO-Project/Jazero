package dk.aau.cs.dkwe.edao.calypso.datalake.structures;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DynamicTableTest extends TableTest
{
    private static final String[] ATTRIBUTES = {"attr1", "attr2", "attr3"};

    @Override
    public Table<Integer> setup()
    {
        return new DynamicTable<>(ATTRIBUTES);
    }

    protected String[] attributes()
    {
        return ATTRIBUTES;
    }

    @Test
    public void testDynamicRows()
    {
        Table<Integer> table = setup();
        Table.Row<Integer> r1 = new Table.Row<Integer>(1, 2, 3),
                r2 = new Table.Row<>(1, 2, 3, 4, 5),
                r3 = new Table.Row<Integer>(1);
        table.addRow(r1);
        table.addRow(r2);
        table.addRow(r3);

        assertEquals(r1, table.getRow(0));
        assertEquals(r2, table.getRow(1));
        assertEquals(r3, table.getRow(2));
    }
}
