package dk.aau.cs.dkwe.edao.jazero.datalake.structures;

import java.io.Serializable;

public record PairNonComparable<F, S>(F first, S second) implements Serializable
{
    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PairNonComparable))
        {
            return false;
        }

        PairNonComparable<?, ?> other = (PairNonComparable<?, ?>) o;
        return this.first.equals(other.first()) && this.first.equals(other.second());
    }
}
