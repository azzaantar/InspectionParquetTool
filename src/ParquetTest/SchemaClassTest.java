package ParquetTest;

import static org.junit.Assert.*;

import org.junit.Test;

import parquet.SchemaClass;

public class SchemaClassTest {
    String[] Schemas= {"HR","Sales","Audit"};
    SchemaClass schemaobject=new SchemaClass();
//    @Test
//    public void testReturnListSchema() {
//        fail("Not yet implemented");
//    }

    @Test
    public void testSortByName() {
        // fail("Not yet implemented");
       String[] sortedNames= schemaobject.sortByName(Schemas);
       
        String[] sortedNamesExpected= {"Audit","HR","Sales"};
        assertArrayEquals(sortedNamesExpected, sortedNames);
    }

//    @Test
//    public void testGetRowCount() {
//        fail("Not yet implemented");
//    }

}
