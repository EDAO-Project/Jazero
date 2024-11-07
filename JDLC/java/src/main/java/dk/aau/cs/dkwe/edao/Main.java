package dk.aau.cs.dkwe.edao;

import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;

public class Main
{
    public static void main(String[] args)
    {
        Table<String> table = new DynamicTable<>("col1", "col2", "col3");
    }
}