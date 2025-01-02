package dk.aau.cs.dkwe.edao.jazero.datalake.structures.table;

import java.util.ArrayList;
import java.util.List;

public class SimpleTable<T> implements Table<T>
{
    private final List<Row<T>> table;
    private final List<String> labels;
    private String id;

    public SimpleTable(String ... columnLabels)
    {
        this.id = "";
        this.labels = List.of(columnLabels);
        this.table = new ArrayList<>();
    }

    public SimpleTable(List<List<T>> table, String ... columnLabels)
    {
        this(columnLabels);

        for (List<T> row : table)
        {
            this.table.add(new Row<>(row));
        }
    }

    public SimpleTable(String id, String ... columnLabels)
    {
        this(columnLabels);
        this.id = id;
    }

    public SimpleTable(String id, List<List<T>> table, String ... columnLabels)
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
        if (this.table.isEmpty() || row.size() == this.table.get(this.table.size() - 1).size())
            this.table.add(row);

        else
            throw new IllegalArgumentException("Row size mismatch");
    }

    @Override
    public int rowCount()
    {
        return this.table.size();
    }

    @Override
    public int columnCount()
    {
        return this.labels.size();
    }

    @Override
    public void removeRow(int index)
    {
        if (index < 0 || index >= this.table.size())
        {
            throw new IllegalArgumentException("Index out of range");
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
        if (!(o instanceof SimpleTable<?>))
        {
            return false;
        }

        SimpleTable<T> other = (SimpleTable<T>) o;
        int thisRows = rowCount(), otherRows = other.rowCount();

        if (thisRows != otherRows)
        {
            return false;
        }

        for (int row = 0; row < thisRows; row++)
        {
            if (!getRow(row).equals(other.getRow(row)))
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
