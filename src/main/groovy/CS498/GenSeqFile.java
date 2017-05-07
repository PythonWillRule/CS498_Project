import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;

/**
 * Created by gumpy on 5/4/17.
 */
public class GenSeqFile {
    private OUTPUT_FILENAME = "sample.dat"
    public static void main(String[] args) {
        System.out.println("Attempting to write sequence file");
        String dicomImagePath = args[0];

        File node = new File(dicomImagePath);
        if(node.isxxx)


    }//main()
}//GenSeqFile CLASS