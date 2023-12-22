package dk.aau.cs.dkwe.edao.jazero.datalake.structures;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
                r3 = new Table.Row<>(1);
        table.addRow(r1);
        table.addRow(r2);
        table.addRow(r3);

        assertEquals(r1, table.getRow(0));
        assertEquals(r2, table.getRow(1));
        assertEquals(r3, table.getRow(2));
    }

    @Test
    public void testEquals()
    {
        Table<Integer> table1 = setup(), table2 = setup();
        Table.Row<Integer> r1 = new Table.Row<Integer>(1, 2, 3),
                r2 = new Table.Row<>(1, 2, 3, 4, 5),
                r3 = new Table.Row<>(1);
        table1.addRow(r1);
        table1.addRow(r2);
        table1.addRow(r3);
        table2.addRow(r1);
        table2.addRow(r2);
        table2.addRow(r3);

        assertTrue(table1.equals(table2));
    }

    @Test
    public void testNotEquals1()
    {
        Table<Integer> table1 = setup(), table2 = setup();
        Table.Row<Integer> r1 = new Table.Row<Integer>(1, 2, 3),
                r2 = new Table.Row<>(1, 2, 3, 4, 5),
                r3 = new Table.Row<>(1);
        table1.addRow(r1);
        table1.addRow(r2);
        table1.addRow(r3);
        table2.addRow(r1);

        assertFalse(table1.equals(table2));
    }

    @Test
    public void testNotEquals2()
    {
        Table<Integer> table1 = setup(), table2 = setup();
        Table.Row<Integer> r1 = new Table.Row<Integer>(1, 2, 3),
                r2 = new Table.Row<>(1, 2, 3, 4, 5),
                r3 = new Table.Row<>(1);
        table1.addRow(r1);
        table1.addRow(r2);
        table1.addRow(r3);
        table2.addRow(r1);
        table2.addRow(r2);
        table2.addRow(new Table.Row<>(1, 2));

        assertFalse(table1.equals(table2));
    }
}
