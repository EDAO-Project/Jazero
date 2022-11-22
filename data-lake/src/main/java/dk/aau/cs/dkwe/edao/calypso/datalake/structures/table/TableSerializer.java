package dk.aau.cs.dkwe.edao.calypso.datalake.structures.table;

import dk.aau.cs.dkwe.edao.calypso.datalake.utilities.Serializer;

/** Serializes a tables according to SDL Manager requirement
 * Tuples are separated by '#' and each tuple element is separated by '<>'
 */
public class TableSerializer<E> extends Serializer
{
    private Table<E> table;

    public static <E> TableSerializer create(Table<E> table)
    {
        return new TableSerializer(table);
    }

    private TableSerializer(Table<E> table)
    {
        this.table = table;
    }

    @Override
    protected String abstractSerialize()
    {
        StringBuilder builder = new StringBuilder();
        int rows = this.table.rowCount();

        for (int row = 0; row < rows; row++)
        {
            int columns = this.table.getRow(row).size();    // Number of columns is not required to be fixed, so we compute it for each row

            for (int column = 0; column < columns; column++)
            {
                builder.append(this.table.getRow(row).get(column)).append("<>");
            }

            if (columns > 0)
            {
                builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1).append("#");
            }
        }

        if (rows > 0)
        {
            builder.deleteCharAt(builder.length() - 1);
        }

        return builder.toString();
    }
}
