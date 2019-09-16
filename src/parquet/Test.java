package parquet;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;

public class Test {
    static SchemaClass obSchema = new SchemaClass();

    public static void main(String args[]) {
        
//        if (args.length == 0) {
//            System.out.println("please enter tenant Path");
//            } else {
//           
//            System.out.println(args[0]);
//            File tenantPath = new File(args[0]);
//            obSchema.returnListSchema(tenantPath);
//            }
        String log4jConfPath = "log4j/log4j.properties";
        PropertyConfigurator.configure(log4jConfPath);
        File file=new File("/Users/azzaanter/IncortaAnalytics/Tenants/dev");
//        BasicConfigurator.configure();
        
        obSchema.returnListSchema(file);
        
    }

}
