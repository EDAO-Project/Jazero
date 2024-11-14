package dk.aau.cs.dkwe.edao.structures;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class TableQuery implements Query
{
    private Table<String> query;

    public TableQuery(Table<String> query)
    {
        this.query = query;
    }

    public TableQuery(InputStream deserializerStream) throws IOException
    {
        deserialize(deserializerStream);
    }

    @Override
    public InputStream serialize()
    {
        StringBuilder builder = new StringBuilder();
        int rows = this.query.rowCount(), columns = this.query.columnCount();

        for (int row = 0; row < rows; row++)
        {
            Table.Row<String> queryRow = this.query.getRow(row);

            for (int column = 0; column < columns; column++)
            {
                String cell = queryRow.get(column);
                builder.append(cell);

                if (column < columns - 1)
                {
                    builder.append("<>");
                }
            }

            if (row < rows - 1)
            {
                builder.append("#");
            }
        }

        return new ByteArrayInputStream(builder.toString().getBytes());
    }

    private void deserialize(InputStream stream) throws IOException
    {
        List<List<String>> table = new ArrayList<>();
        byte[] buffer = stream.readAllBytes();
        String str = new String(buffer);
        String[] strRows = str.split("#");

        for (String strRow : strRows)
        {
            String[] cells = strRow.split("<>");
            List<String> row = new ArrayList<>(List.of(cells));
            table.add(row);
        }

        this.query = new DynamicTable<>(table);
    }

    public Table<String> getQuery()
    {
        return this.query;
    }

    @Override
    public String toString()
    {
        try (InputStream stream = serialize())
        {
            byte[] buffer = stream.readAllBytes();
            return new String(buffer);
        }

        catch (IOException e)
        {
            return null;
        }
    }
}
