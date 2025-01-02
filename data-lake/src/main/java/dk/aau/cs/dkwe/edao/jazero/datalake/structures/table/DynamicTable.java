package dk.aau.cs.dkwe.edao.jazero.datalake.structures.table;

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
    private String id;

    public DynamicTable(List<String> columnLabels)
    {
        this.id = "";
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

    public DynamicTable(String id, List<String> columnLabels)
    {
        this(columnLabels);
        this.id = id;
    }

    public DynamicTable(String id, String ... columnLabels)
    {
        this(columnLabels);
        this.id = id;
    }

    public DynamicTable(String id, List<List<T>> table, List<String> columnLabels)
    {
        this(table, columnLabels);
        this.id = id;
    }

    public DynamicTable(String id, List<List<T>> table, String ... columnLabels)
    {
        this(table, columnLabels);
        this.id = id;
    }

    @Override
    public String getId()
    {
        return this.id;
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
    public void removeRow(int index)
    {
        if (index < 0 || index >= this.table.size())
        {
            throw new IllegalArgumentException("Index out of bounds");
        }

        this.table.remove(index);
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

    @Override
    public int compareTo(Table<String> other)
    {
        return Integer.compare(rowCount(), other.rowCount());
    }

    @Override
    public List<List<T>> toList()
    {
        List<List<T>> table = new ArrayList<>();

        for (Row<T> row : this.table)
        {
            List<T> listRow = new ArrayList<>();
            row.forEach(listRow::add);
            table.add(listRow);
        }

        return table;
    }
}
