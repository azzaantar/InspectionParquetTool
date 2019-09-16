package ParquetTest;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.SchemaClass;

public class SchemaClassTest {
    String[] Schemas = { "HR", "Sales", "Audit" };
    SchemaClass schemaobject = new SchemaClass();
    Path file = new Path("/Users/azzaanter/IncortaAnalytics/Tenants/dev/parquet/HR/DEPARTMENTS/part-000000");
    // @Test
    // public void testReturnListSchema() {
    // fail("Not yet implemented");
    // }

    @Test
    public void testSortByName() {
        // fail("Not yet implemented");
        String[] sortedNames = schemaobject.sortByName(Schemas);

        String[] sortedNamesExpected = { "Audit", "HR", "Sales" };
        assertArrayEquals(sortedNamesExpected, sortedNames);
    }

    @Test
    public void testGetRowCount() {

        // fail("Not yet implemented");
        long rowCount = schemaobject.getRowCount(file);
        long expectedRowCount = 11;
        assertEquals(expectedRowCount, rowCount);

    }

}
