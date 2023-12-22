package dk.aau.cs.dkwe.edao.jazero.datalake.parser;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.tables.JsonTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.utilities.Utils;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableParser
{
    public static Table<String> toTable(File f)
    {
        try
        {
            Gson gson = new Gson();
            Reader reader = Files.newBufferedReader(f.toPath());
            Type type = new TypeToken<HashMap<String, List<List<String>>>>(){}.getType();
            Map<String, List<List<String>>> map = gson.fromJson(reader, type);

            return new DynamicTable<>(map.get("queries"));
        }

        catch (IOException e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public static Table<String> toTable(List<List<String>> matrix)
    {
        return new DynamicTable<>(matrix);
    }

    public static JsonTable parse(Path path)
    {
        JsonTable table = Utils.getTableFromPath(path);
        return table == null || table._id  == null || table.rows == null ? null : table;
    }

    public static Table<String> parse(File file)
    {
        JsonTable jTable = parse(file.toPath());

        if (jTable == null || jTable.numDataRows == 0)
        {
            return null;
        }

        List<String> headers = jTable.headers
                .stream()
                .map(cell -> cell.text)
                .toList();
        List<List<String>> rows = jTable.rows.stream()
                .map(row -> row.stream()
                        .map(cell -> cell.text)
                .collect(Collectors.toList())).collect(Collectors.toList());
        return new DynamicTable<>(rows, headers);
    }
}
