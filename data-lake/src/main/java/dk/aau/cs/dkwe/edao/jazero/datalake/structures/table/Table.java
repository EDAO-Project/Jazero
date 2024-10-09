package dk.aau.cs.dkwe.edao.jazero.datalake.structures.table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public interface Table<T> extends Iterable<T>
{
    Row<T> getRow(int index);
    Column<T> getColumn(int index);
    Column<T> getColumn(String label);
    String[] getColumnLabels();
    void addRow(Row<T> row);
    int rowCount();
    int columnCount();
    void removeRow(int index);

    default String toStr()
    {
        StringBuilder builder = new StringBuilder("[");
        int size = rowCount();

        for (int i = 0; i < size; i++)
        {
            builder.append(getRow(i)).append(", ");
        }

        builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1).append("]");
        return builder.toString();
    }

    @Override
    default Iterator<T> iterator()
    {
        int rowCount = rowCount();
        List<Iterator<T>> iterators = new ArrayList<>();

        for (int i = 0; i < rowCount; i++)
        {
            iterators.add(getRow(i).iterator());
        }

        return new MultiIterator<>(iterators);
    }

    record Row<E>(List<E> row) implements Iterable<E>
    {
        public Row(E ... elements)
        {
            this(List.of(elements));
        }

        public E get(int index)
        {
            return this.row.get(index);
        }

        public int size()
        {
            return this.row.size();
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Row<?> other))
                return false;

            return this.row.equals(other.row);
        }

        @Override
        public String toString()
        {
            if (this.row.isEmpty())
            {
                return "[]";
            }

            StringBuilder builder = new StringBuilder("[");

            for (E e : this.row)
            {
                builder.append(e.toString()).append(", ");
            }

            builder.deleteCharAt(builder.length() - 1).deleteCharAt(builder.length() - 1).append("]");
            return builder.toString();
        }

        @Override
        public Iterator<E> iterator()
        {
            return this.row.iterator();
        }
    }

    record Column<E>(String label, List<E> elements) implements Iterable<E>
    {
        public Column(String label, E ... columnElements)
        {
            this(label, List.of(columnElements));
        }

        public String getLabel()
        {
            return this.label;
        }

        public E get(int row)
        {
            return this.elements.get(row);
        }

        public int size()
        {
            return this.elements.size();
        }

        @Override
        public Iterator<E> iterator()
        {
            return this.elements.iterator();
        }
    }
}
