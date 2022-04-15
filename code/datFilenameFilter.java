package code;

import java.io.File;
import java.io.FilenameFilter;

public class datFilenameFilter implements FilenameFilter {

    @Override
    public boolean accept(File dir, String name) {
        // file filter only use file end with .dat
        return name.endsWith(".dat");
    }
    
}
