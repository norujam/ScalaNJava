package future;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class ZipFileHandler implements Callable<Map<String, String>>
{
    private File logFile;

    public ZipFileHandler(File logFile) {
        this.logFile = logFile;
    }

    @Override
    public Map<String, String> call() {
        System.out.println(Thread.currentThread().getName());
        ZipInputStream zipIn = null;
        ZipEntry entry = null;
        Map<String, String> result = new HashMap<>();
        try {
            zipIn = new ZipInputStream(new FileInputStream(logFile));
            entry = zipIn.getNextEntry();
            while (entry != null) {
                String filePath = "c:/temp/unzipFile" + File.separator + entry.getName();
                if (!entry.isDirectory()) {
                    extractFile(zipIn, filePath);
                } else {
                    File dir = new File(filePath);
                    dir.mkdir();
                }
                zipIn.closeEntry();
                entry = zipIn.getNextEntry();
            }
        } catch (Exception e) {
            e.printStackTrace();
            result.put("status", "Error-1");
            result.put("fileName", logFile.getAbsolutePath());
            return result;
        } finally {
            if(zipIn!=null) {
                try {
                    zipIn.closeEntry();
                    zipIn.close();
                } catch (IOException ie) {
                    ie.printStackTrace();
                }
            }
        }

        result.put("status", "1");
        result.put("fileName", logFile.getAbsolutePath());

        return result;
    }

    private void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
        byte[] bytesIn = new byte[1024];
        int read = 0;
        while ((read = zipIn.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }
}