package parquet;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

public class SchemaClass {

    static SchemaClass obSchema = new SchemaClass();

    // #1 return List of schemas
    public String[] returnListSchema(File tenantPath) {

        String[] schemasList = null;
        String[] tablesName = null;
        String[] partsList = null;
        String strDate = null;
        long fileSize = 0;
        long TotalRowCount = 0;
        String[] directoriesP = tenantPath.list(new FilenameFilter() {
            @Override
            public boolean accept(File current, String name) {
                return new File(current, name).isDirectory();
            }
        });
        String parquetSearch = "parquet";

        boolean isExists = false;

        for (int i = 0; i < directoriesP.length; i++) {

            String arrayValue = directoriesP[i];

            if (parquetSearch.equals(arrayValue)) {
                isExists = true;

                break;
            }
        }
        if (isExists) {
            File parquetPath = new File(tenantPath + "/" + parquetSearch);
            schemasList = parquetPath.list(new FilenameFilter() {
                @Override
                public boolean accept(File current, String name) {
                    return new File(current, name).isDirectory();
                }
            });
            sortByName(schemasList);
            for (int i = 0; i < schemasList.length; i++) {
                File tablePath = new File(parquetPath + "/" + schemasList[i]);
                tablesName = tablePath.list(new FilenameFilter() {
                    @Override
                    public boolean accept(File current, String name) {
                        return new File(current, name).isDirectory();
                    }
                });

                sortByName(tablesName);
                System.out.println(schemasList[i] + " schema\n");
                for (int j = 0; j < tablesName.length; j++) {

                    if (!tablesName[j].contains(".")) {
                        System.out.print("\t" + tablesName[j] + "\t[Full" + " , ");
                        File PartsPath = new File(parquetPath + "/" + schemasList[i] + "/" + tablesName[j]);
                        partsList = PartsPath.list(new FilenameFilter() {
                            @Override
                            public boolean accept(File current, String name) {
                                boolean fileCondition = new File(current, name).isFile();
                                boolean partCondition = new File(current, name).getName().contains("part");
                                boolean crcCondition = new File(current, name).getName().contains(".crc");
                                boolean offsetsCondition = new File(current, name).getName().contains(".offsets");
                                boolean flag = false;
                                if ((fileCondition & partCondition) & (!crcCondition & !offsetsCondition)) {
                                    flag = true;
                                }
                                return flag;

                            }
                        });
                        for (int t = 0; t < partsList.length; t++) {
                            File fileParqu = new File(PartsPath + "/" + partsList[t]);
                            fileSize = +FileUtils.sizeOf(fileParqu);
                            Path ParquetPart = new Path(
                                    parquetPath + "/" + schemasList[i] + "/" + tablesName[j] + "/" + partsList[t]);
                            TotalRowCount = TotalRowCount + getRowCount(ParquetPart);

                        }
                        if (partsList.length == 0) {
                            fileSize = 0;
                            TotalRowCount = 0;
                        }

                        System.out.println(fileSize + "kB, " + partsList.length + " part" + ", " + TotalRowCount
                                + " rows ]" + "\n");

                    }

                    else {
                        String[] words = tablesName[j].split("\\.");
                        for (int f = 0; f < words.length; f++) {
                            long milliSecconds = Long.parseLong(words[1]);
                            Date date = new Date(milliSecconds);

                            SimpleDateFormat formatter = new SimpleDateFormat("yyyy.MM.dd  HH.mm.ss");
                            strDate = formatter.format(date);

                        }
                        System.out.print("\t   " + tablesName[j] + "\t[Update , " + strDate + " ,");
                        File PartsPath = new File(parquetPath + "/" + schemasList[i] + "/" + tablesName[j]);
                        partsList = PartsPath.list(new FilenameFilter() {
                            @Override
                            public boolean accept(File current, String name) {
                                boolean fileCondition = new File(current, name).isFile();
                                boolean partCondition = new File(current, name).getName().contains("part");
                                boolean flag = false;
                                if (fileCondition & partCondition) {
                                    flag = true;
                                }
                                return flag;

                            }
                        });
                        for (int t = 0; t < partsList.length; t++) {
                            File fileParqu = new File(PartsPath + "/" + partsList[t]);
                            fileSize = +FileUtils.sizeOf(fileParqu);
                            Path ParquetPart = new Path(
                                    parquetPath + "/" + schemasList[i] + "/" + tablesName[j] + "/" + partsList[t]);
                            TotalRowCount = TotalRowCount + getRowCount(ParquetPart);

                        }
                        if (partsList.length == 0) {
                            fileSize = 0;
                            TotalRowCount = 0;
                        }

                        System.out.println(fileSize + "kB , " + partsList.length + " part" + ", " + TotalRowCount
                                + " rows ] " + "\n");

                    }
                    TotalRowCount = 0;
                }

            }

        } else

        {
            System.out.println("Parquet is not found in the tenant");
        }

        return

        sortByName(schemasList);
    }
    // method to sort by name

    public String[] sortByName(String[] ListSchemas) {
        String temp;
        for (int i = 0; i < ListSchemas.length; i++) {
            for (int j = i + 1; j < ListSchemas.length; j++) {
                if (ListSchemas[i].compareTo(ListSchemas[j]) > 0) {
                    temp = ListSchemas[i];
                    ListSchemas[i] = ListSchemas[j];
                    ListSchemas[j] = temp;
                }
            }
        }
        return ListSchemas;
    }

    // method to get row count
    public long getRowCount(Path file) {
        long rowcounts = 0;
        try {
            FileSystem fs = file.getFileSystem(new Configuration());
            List<FileStatus> statuses = Arrays.asList(fs.listStatus(file, HiddenFileFilter.INSTANCE));
            List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(new Configuration(),
                    statuses, false);

            for (Footer footer : footers) {
                for (BlockMetaData block : footer.getParquetMetadata().getBlocks()) {
                    rowcounts += block.getRowCount();
                }
            }
        }

        catch (IOException e) {
            e.printStackTrace();
        }
        return rowcounts;

    }

}
