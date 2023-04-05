package dk.aau.cs.dkwe.edao.calypso.datalake.structures;

import java.io.Serializable;
import java.util.Objects;

public record Pair<F extends Comparable<F>, S extends Comparable<S>>(F first, S second) implements Serializable, Comparable<Pair<F, S>>
{
    public Pair
    {
        if (first == null)
        {
            throw new NullPointerException("Null value for Pair.first is not allowed");
        }

        if (second == null)
        {
            throw new NullPointerException("Null value for Pair.second is not allowed");
        }
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == null)
        {
            return  false;
        }

        if (other == this)
        {
            return true;
        }

        if (!(other instanceof Pair<?, ?> m))
        {
            return false;
        }

        try
        {
            F _first = (F) m.first();
            S _second = (S) m.second();
            return Objects.equals(this.first(), _first) && Objects.equals(this.second(), _second);
        }

        catch (ClassCastException | NullPointerException unused)
        {
            return false;
        }

    }

    public int hashCode()
    {
        return this.first().hashCode() + 113 * this.second().hashCode();
    }

    @Override
    public int compareTo(Pair<F, S> other)
    {
        if (this.first.compareTo(other.first()) == 0)
        {
            return this.second.compareTo(other.second());
        }

        return this.first.compareTo(other.first());
    }
}
