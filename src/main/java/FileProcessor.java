import java.io.*;

/**
 * Created by edara on 5/14/17.
 */
public class FileProcessor {
    public static void main(String[] args) throws IOException {
        String filename = "/Users/edara/Downloads/test.txt";
        String outDir = "/Users/edara/Downloads/out/";
        int linesCount = 100000;
        long t1 = System.currentTimeMillis();
        splitFiles(filename,outDir,linesCount);


        System.out.println("Done: (sec)"+(System.currentTimeMillis() -t1)/1000);
    }

    // create file chunks of few million lines each.
    public static void splitFiles(String filename, String outDir,int lines) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));
        int lineCount =0;
        int fileCount =1;
        BufferedWriter writer = new BufferedWriter(new FileWriter(outDir+fileCount+".txt",true));
        String temp;
        while((temp=reader.readLine()) != null) {
            lineCount++;
            writer.write(temp);
            if(lineCount >= lines){
                lineCount=0;
                fileCount++;
                writer.close();
                writer = new BufferedWriter(new FileWriter(outDir+fileCount+".txt",true));
            }

        }
        writer.close();

    }
}
