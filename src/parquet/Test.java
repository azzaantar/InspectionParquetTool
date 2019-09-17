package parquet;

import java.io.File;
import org.apache.log4j.PropertyConfigurator;

public class Test {
    static SchemaClass obSchema = new SchemaClass();

    public static void main(String args[]) {

        if (args.length == 0) {
            System.out.println("please enter tenant Path");
        } else {

            System.out.println(args[0]);
            String log4jConfPath = "src/log4j.properties";
            PropertyConfigurator.configure(log4jConfPath);
            File tenantPath = new File(args[0]);
            if (tenantPath.getAbsolutePath().contains("parquet")) {
                obSchema.SchemaName(tenantPath);
            } else {
                obSchema.returnListSchema(tenantPath);
            }

        }

    }

}
