package dk.aau.cs.dkwe.edao.calypso.cdlc.query;

import dk.aau.cs.dkwe.edao.calypso.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;

import java.io.File;
import java.util.List;

public class QueryFactory
{
    public static Query create(File jsonFile)
    {
        return new Query(TableParser.toTable(jsonFile));
    }

    public static Query create(List<List<String>> matrix)
    {
        return new Query(TableParser.toTable(matrix));
    }

    public static Query create(Table<String> table)
    {
        return new Query(table);
    }
}
