package parquet;

import java.io.File;

public class Test {
    static SchemaClass obSchema = new SchemaClass();

    public static void main(String args[]) {
        if (args.length == 0) {
            System.out.println("please enter tenant Path");
        } else {

            System.out.println(args[0]);
            File tenantPath = new File(args[0]);
            obSchema.returnListSchema(tenantPath);
        }
    }

}
