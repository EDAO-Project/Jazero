package structures;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.structures.Query;
import dk.aau.cs.dkwe.edao.structures.TableQuery;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TableQueryTest
{
    @Test
    public void testSerialize()
    {
        List<List<String>> matrix = List.of(List.of("cell11", "cell12"), List.of("cell21", "cell22"));
        Query query = new TableQuery(new DynamicTable<>(matrix));

        try (InputStream stream = query.serialize())
        {
            byte[] buffer = stream.readAllBytes();
            String queryString = new String(buffer);
            assertEquals("cell11<>cell12#cell21<>cell22", queryString);
        }

        catch (IOException e)
        {
            fail(e.getMessage());
        }
    }

    @Test
    public void testDeserialize()
    {
        try
        {
            ByteArrayInputStream stream = new ByteArrayInputStream("cell11<>cell12#cell21<>cell22".getBytes());
            TableQuery query = new TableQuery(stream);
            Table<String> table = query.getQuery();
            Table.Row<String> row1 = table.getRow(0), row2 = table.getRow(1);
            assertEquals("cell11", row1.get(0));
            assertEquals("cell12", row1.get(1));
            assertEquals("cell21", row2.get(0));
            assertEquals("cell22", row2.get(1));
        }

        catch (IOException e)
        {
            fail(e.getMessage());
        }
    }
}
