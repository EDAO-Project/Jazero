package dk.aau.cs.dkwe.edao.calypso.datalake.store;

import java.io.Serializable;

public record SynchronizedLinker<F, T>(Linker<F, T> linker) implements Linker<F, T>, Serializable
{
    public static <From, To> SynchronizedLinker<From, To> wrap(Linker<From, To> linker)
    {
        return new SynchronizedLinker<>(linker);
    }

    @Override
    public synchronized T mapTo(F from)
    {
        return this.linker.mapTo(from);
    }

    @Override
    public synchronized F mapFrom(T to)
    {
        return this.linker.mapFrom(to);
    }

    @Override
    public synchronized void addMapping(F from, T to)
    {
        this.linker.addMapping(from, to);
    }

    @Override
    public void clear()
    {
        this.linker.clear();
    }
}
