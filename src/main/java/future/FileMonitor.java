package future;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class FileMonitor {
    static private File[] fileList;

    public static void main(String[] args) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
        List<Future<Map<String, String>>> resultList = new ArrayList<>();
        List<File> targetList = Collections.synchronizedList(new ArrayList<>());
        List<File> runningList = Collections.synchronizedList(new ArrayList<>());

        File errFile = new File("C:/temp/errFile");
        if(!errFile.isDirectory())
            errFile.mkdir();

        File unzipFile = new File("C:/temp/unzipFile");
        if(!unzipFile.isDirectory())
            unzipFile.mkdir();

        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                long startTime = System.currentTimeMillis();
                System.out.println("Monitor is running...");
                File targetDir = new File("c:/temp");
                if(targetDir.isDirectory()) {
                    fileList = targetDir.listFiles();
                    if(fileList==null)
                        return;

                    for(File file : fileList) {
                        if(file.isFile() && targetList.size() < 4 && !targetList.contains(file) && file.getName().toLowerCase().contains(".zip")) {
                            targetList.add(file);
                        }
                    }
                }

                if(targetList.isEmpty())
                    return;

                for(File file : targetList) {
                    if(!runningList.contains(file)) {
                        ZipFileHandler logDataHandler = new ZipFileHandler(file);
                        Future<Map<String, String>> result = executor.submit(logDataHandler);
                        runningList.add(file);
                        resultList.add(result);
                    }
                }

                for(Future<Map<String, String>> future : resultList) {
                    try {
                        System.out.println("future " + future.get().get("fileName") + "; done? " + future.isDone());
                        if(future.get().get("status").equals("1") && future.isDone()) {
                            File doneFile = new File(future.get().get("fileName"));
                            runningList.remove(doneFile);
                            targetList.remove(doneFile);
                            doneFile.delete();
                        } else {
                            File errorFile = new File(future.get().get("fileName"));
                            copyFile(errorFile, new File("c:/temp/errFile/"));
                            runningList.remove(errorFile);
                            targetList.remove(errorFile);
                            errorFile.delete();
                        }
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    } catch (Exception ie) {
                        ie.printStackTrace();
                    }
                }
                long stopTime = System.currentTimeMillis();
                long elapsedTime = stopTime - startTime;
                System.out.println("run time="+elapsedTime);
            }
        };


        Timer timer = new Timer();
        long delay = 0;
        long intevalPeriod = 1 * 10000;
        // schedules the task to be run in an interval
        timer.scheduleAtFixedRate(task, delay, intevalPeriod);
    }

    private static void copyFile(File source, File target) throws Exception {
        if(source.isDirectory()){
            if(!target.isDirectory()){
                target.mkdirs();
            }
            String[] children  = source.list();
            for(int i=0; i<children.length; i++){
                copyFile(new File(source, children[i]),new File(target, children[i]));
            }
        }else{

            int bufferSize = 8192;//(64 * 1024 * 1024) - (32 * 1024);

            ScatteringByteChannel inChannel = new FileInputStream(source).getChannel();
            if(target.isDirectory()) {
                System.out.println(target.getAbsolutePath());
                target = new File(target.getAbsolutePath()+File.separator+source.getName());
            }
            GatheringByteChannel outChannel = new FileOutputStream(target).getChannel();
            try {
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bufferSize);
                while (inChannel.read(byteBuffer) != -1) {
                    byteBuffer.flip();
                    outChannel.write(byteBuffer);
                    byteBuffer.clear();
                }
            }catch (Exception e) {
                throw e;
            }finally {
                if (inChannel != null)
                    inChannel.close();
                if (outChannel != null)
                    outChannel.close();
            }
        }
    }
}