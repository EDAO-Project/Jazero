package dk.aau.cs.dkwe.edao.jazero.datalake.structures;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.TableDeserializer;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TableDeserializerTest
{
    @Test
    public void testOneRow()
    {
        Table<String> deserialized = TableDeserializer.create("element1<>element2<>element3").deserialize();
        Table<String> expected = new DynamicTable<>(List.of(List.of("element1", "element2", "element3")));
        assertEquals(expected, deserialized);
    }

    @Test
    public void testOneRowOneColumn()
    {
        Table<String> deserialized = TableDeserializer.create("element").deserialize();
        Table<String> expected = new DynamicTable<>(List.of(List.of("element")));
        assertEquals(expected, deserialized);
    }

    @Test
    public void testTwoRows()
    {
        Table<String> deserialized = TableDeserializer.create("element1<>element2<>element3#element4<>element5<>element6").deserialize();
        Table<String> expected = new DynamicTable<>(List.of(List.of("element1", "element2", "element3"), List.of("element4", "element5", "element6")));
        assertEquals(expected, deserialized);
    }

    @Test
    public void testTwoRowsOneColumn()
    {
        Table<String> deserialized = TableDeserializer.create("element1#element2").deserialize();
        Table<String> expected = new DynamicTable<>(List.of(List.of("element1"), List.of("element2")));
        assertEquals(expected, deserialized);
    }

    @Test
    public void bigTableTest()
    {
        Table<String> deserialized = TableDeserializer.create("element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2#element1<>element2#element1<>element2#element1<>element2#" +
                "element1<>element2#element1<>element2").deserialize();
        Table<String> expected = new DynamicTable<>(List.of(List.of("element1", "element2"), List.of("element1", "element2"),
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
        assertEquals(expected, deserialized);
    }

    @Test
    public void emptyRow()
    {
        Table<String> deserialized = TableDeserializer.create("element1<>element2##element3<>element4").deserialize();
        Table<String> expected = new DynamicTable<>(List.of(List.of("element1", "element2"), List.of(), List.of("element3", "element4")));
        assertEquals(expected, deserialized);
    }
}
