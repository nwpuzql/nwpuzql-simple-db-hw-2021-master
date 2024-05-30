package simpledb.storage;

public class Table {       //create a helper class Table to better understand how catalog works
    private DbFile file;
    private String name;
    private String pkeyField;  //each table contains a Dbfile, a name ,and a pkeyField

    public Table(DbFile file, String name, String pkeyField) {
        this.file = file;
        this.name = name;
        this.pkeyField = pkeyField;
    }

    public DbFile getFile() {
        return file;
    }

    public void setFile(DbFile file) {
        this.file = file;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPkeyField() {
        return pkeyField;
    }

    public void setPkeyField(String pkeyField) {
        this.pkeyField = pkeyField;
    }
}
