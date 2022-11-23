package dk.aau.cs.dkwe.edao.calypso.datalake.structures;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.TableDeserializer;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.TableSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableSerializerTest
{
    @Test
    public void testOneRow()
    {
        Table<String> table = new DynamicTable<>(List.of(List.of("element1", "element2", "element3")));
        String serialized = TableSerializer.create(table).serialize();
        assertEquals("element1<>element2<>element3", serialized);
    }

    @Test
    public void testOneRowOneColumn()
    {
        Table<String> table = new DynamicTable<>(List.of(List.of("element")));
        String serialized = TableSerializer.create(table).serialize();
        assertEquals("element", serialized);
    }

    @Test
    public void testTwoRows()
    {
        Table<String> table = new DynamicTable<>(List.of(List.of("element1", "element2", "element3"), List.of("element4", "element5", "element6")));
        String serialized = TableSerializer.create(table).serialize();
        assertEquals("element1<>element2<>element3#element4<>element5<>element6", serialized);
    }

    @Test
    public void testTwoRowsOneColumn()
    {
        Table<String> table = new DynamicTable<>(List.of(List.of("element1"), List.of("element2")));
        String serialized = TableSerializer.create(table).serialize();
        assertEquals("element1#element2", serialized);
    }

    @Test
    public void bigTableTest()
    {
        Table<String> table = new DynamicTable<>(List.of(List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2"),
                List.of("element1", "element2"), List.of("element1", "element2"), List.of("element1", "element2")));
        String serialized = TableSerializer.create(table).serialize();
        assertEquals("element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2", serialized);
    }

    @Test
    public void emptyRow()
    {
        Table<String> table = new DynamicTable<>(List.of(List.of("element1", "element2"), List.of(), List.of("element3", "element4")));
        String serialized = TableSerializer.create(table).serialize();
        assertEquals("element1<>element2##element3<>element4", serialized);
    }
}
