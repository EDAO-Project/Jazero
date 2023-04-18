package dk.aau.cs.dkwe.edao.calypso.cdlc.query;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;

public class Query
{
    private Table<String> queryTable;
    private int rows;

    // It is package private to force the user to use the QueryFactory class
    Query(Table<String> queryTable)
    {
        this.queryTable = queryTable;
        this.rows = this.queryTable.rowCount();

    }

    public Table<String> asTable()
    {
        return this.queryTable;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        for (int row = 0; row < this.rows; row++)
        {
            int columns = this.queryTable.getRow(row).size();    // Number of columns is not required to be fixed, so we compute it for each row

            for (int column = 0; column < columns; column++)
            {
                builder.append(this.queryTable.getRow(row).get(column)).append("<>");
            }

            builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() -1).append("#");
        }

        builder.deleteCharAt(builder.length() - 1);
        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof Query))
        {
            return false;
        }

        Query other = (Query) o;
        return this.queryTable.equals(other.asTable());
    }
}
