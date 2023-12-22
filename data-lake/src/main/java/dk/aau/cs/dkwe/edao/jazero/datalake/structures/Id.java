package dk.aau.cs.dkwe.edao.jazero.datalake.structures;

import dk.aau.cs.dkwe.edao.jazero.datalake.system.Configuration;

import java.io.Serializable;

public record Id(int id) implements Serializable, Comparable<Id>
{
    private static class IdAllocator
    {
        private static int allocatedId = -1;

        public Id allocId()
        {
            if (allocatedId == -1)
            {
                String id = Configuration.getLargestId();

                if (id == null)
                    allocatedId = 0;

                else
                    allocatedId = Integer.parseInt(id) + 1;
            }

            Configuration.setLargestId(String.valueOf(allocatedId));
            return Id.copy(allocatedId++);
        }
    }

    public static Id copy(int id)
    {
        return new Id(id);
    }

    /**
     * Only run-time unique
     * @return New run-time unique identifier
     */
    public static Id alloc()
    {
        return new IdAllocator().allocId();
    }

    public static Id any()
    {
        return new Id(-1);
    }

    public Id(int id)
    {
        this.id = id;
        IdAllocator.allocatedId = id + 1;
    }

    @Override
    public int hashCode()
    {
        return this.id;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Id otherId))
            return false;

        return this.id == otherId.id;
    }

    @Override
    public int compareTo(Id other)
    {
        if (this.id == other.id())
            return 0;

        return this.id < other.id() ? -1 : 1;
    }
}
