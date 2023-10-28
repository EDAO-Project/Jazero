package java.dk.aau.cs.dkwe.edao.calypso.datalake.system;

public record User(String username, String password, boolean readOnly)
{
    @Override
    public String toString()
    {
        return this.username + "(" + this.readOnly + "): " + this.password;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof User))
        {
            return false;
        }

        User other = (User) o;
        return this.username.equals(other.username()) &&
                this.password.equals(other.password()) &&
                this.readOnly == other.readOnly();
    }
}
