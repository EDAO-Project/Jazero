package dk.aau.cs.dkwe.edao.calypso.datalake.structures.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Synchronized table with varying row lengths
 * @param <T>
 */
public class DynamicTable<T> implements Table<T>
{
    private final List<Row<T>> table;
    private final List<String> labels;

    public DynamicTable(List<String> columnLabels)
    {
        this.labels = columnLabels;
        this.table = new Vector<>();    // Vector does the synchronization for us
    }

    public DynamicTable(String ... columnLabels)
    {
        this(List.of(columnLabels));
    }

    public DynamicTable(List<List<T>> table, List<String> columnLabels)
    {
        this(columnLabels);

        for (List<T> row : table)
        {
            this.table.add(new Row<>(row));
        }
    }

    public DynamicTable(List<List<T>> table, String ... columnLabels)
    {
        this(table, List.of(columnLabels));
    }

    @Override
    public Row<T> getRow(int index)
    {
        return this.table.get(index);
    }

    @Override
    public Column<T> getColumn(int index)
    {
        List<T> elements = new ArrayList<>(this.table.size());

        for (Row<T> row : this.table)
        {
            if (row.size() > index)
                elements.add(row.get(index));
        }

        return new Column<>(this.labels.get(index), elements);
    }

    @Override
    public Column<T> getColumn(String label)
    {
        if (!this.labels.contains(label))
            throw new IllegalArgumentException("Column label does not exist");

        return getColumn(this.labels.indexOf(label));
    }

    @Override
    public String[] getColumnLabels()
    {
        String[] labels = new String[this.labels.size()];

        for (int i = 0; i < this.labels.size(); i++)
        {
            labels[i] = this.labels.get(i);
        }

        return labels;
    }

    @Override
    public void addRow(Row<T> row)
    {
        this.table.add(row);
    }

    @Override
    public int rowCount()
    {
        return this.table.size();
    }

    /**
     * This does not represent number of row elements since that can be dynamic
     * @return NUmber of attributes given under object construction
     */
    @Override
    public int columnCount()
    {
        return this.table.isEmpty() ? this.labels.size() : this.table.get(0).size();
    }

    @Override
    public String toString()
    {
        return toStr();
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof DynamicTable<?> other))
        {
            return false;
        }

        int thisRows = this.table.size(), otherRows = other.rowCount();

        if (thisRows != otherRows)
        {
            return false;
        }

        for (int row = 0; row < thisRows; row++)
        {
            if (!this.table.get(row).equals(other.getRow(row)))
            {
                return false;
            }
        }

        return true;
    }
}
