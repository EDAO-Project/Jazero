package dk.aau.cs.dkwe.edao.calypso.datalake.structures;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.SimpleTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SimpleTableTest extends TableTest
{
    private static final String[] ATTRIBUTES = {"attr1", "attr2", "attr3"};

    @Override
    protected Table<Integer> setup()
    {
        return new SimpleTable<>(ATTRIBUTES);
    }

    @Override
    protected String[] attributes()
    {
        return ATTRIBUTES;
    }

    @Test
    public void testAddWrongRow()
    {
        try
        {
            Integer[] wrongRow = new Integer[ATTRIBUTES.length + 1];
            Table<Integer> table = setup();
            table.addRow(new Table.Row<>(1, 2, 3));

            for (int i = 0; i < wrongRow.length; i++)
            {
                wrongRow[i] = i + 3;
            }

            table.addRow(new Table.Row<>(wrongRow));   // Wrong number of elements
            fail();
        }

        catch (IllegalArgumentException e) {}
    }

    @Test
    public void testEquals()
    {
        Table<Integer> table1 = setup(), table2 = setup();
        Table.Row<Integer> r1 = new Table.Row<Integer>(1, 2, 3),
                r2 = new Table.Row<>(3, 4, 5),
                r3 = new Table.Row<>(1, 3, 5);
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
                r2 = new Table.Row<>(3, 4, 5),
                r3 = new Table.Row<>(1, 3, 5);
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
                r2 = new Table.Row<>(3, 4, 5),
                r3 = new Table.Row<>(1, 3, 5);
        table1.addRow(r1);
        table1.addRow(r2);
        table1.addRow(r3);
        table2.addRow(r1);
        table2.addRow(r2);
        table2.addRow(new Table.Row<>(1, 2, 1));

        assertFalse(table1.equals(table2));
    }
}
