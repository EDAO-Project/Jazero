package dk.aau.cs.dkwe.edao.jazero.datalake.structures.table;

import dk.aau.cs.dkwe.edao.jazero.datalake.utilities.Deserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Deserialized a table
 * Tuples are separated by '#' and each tuple element is separated by '<>'
 */
public class TableDeserializer extends Deserializer<Table<String>>
{
    private final String serialized;

    public static TableDeserializer create(String serialized)
    {
        return new TableDeserializer(serialized);
    }

    private TableDeserializer(String serialized)
    {
        this.serialized = serialized;
    }

    @Override
    protected Table<String> abstractDeserialize()
    {
        Table<String> table = new DynamicTable<>();
        String[] rowSplit = this.serialized.split("#");

        for (String rowStr : rowSplit)
        {
            if (rowStr.isEmpty())
            {
                table.addRow(new Table.Row<>());
                continue;
            }

            String[] elementSplit = rowStr.split("<>");
            List<String> row = new ArrayList<>(elementSplit.length);
            Collections.addAll(row, elementSplit);
            table.addRow(new Table.Row<>(row));
        }

        return table;
    }
}
