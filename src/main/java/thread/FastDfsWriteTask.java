package thread;

import org.csource.common.MyException;
import org.csource.fastdfs.StorageClient1;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class FastDfsWriteTask implements Callable{
    private Jedis jedis;
    private StorageClient1 storageClient1;
    private  long fileNum;         //单线程写入文件个数
    private  long fileSize;         //写入文件大小
    private int threadOrder;             //线程号
    private  byte[] writeBuffer;
    private  long[] threadTime;
    private CountDownLatch countDownLatch;
    public  FastDfsWriteTask(CountDownLatch countDownLatch,Jedis jedis,StorageClient1 storageClient1,long fileNum,long fileSize,
                         int threadOrder,long[] threadTime ) throws IOException {
        this.countDownLatch=countDownLatch;
        this.jedis=jedis;
        this.storageClient1=storageClient1;
        this.fileNum=fileNum;
        this.threadOrder=threadOrder;
        this.fileSize=fileSize;
        this.threadTime=threadTime;
        this.writeBuffer = new byte[(int)fileSize];//强转，不建议使用
        /*
        初始化写入内容
         */
        for (int i = 0; i < this.fileSize; i++)
            this.writeBuffer[i] = (byte) ('0' + i % 50);
    }
    public StorageClient1 call(){
        System.out.println("FastDFS_WriteThread-" + this.threadOrder + ":  Start to work!");
        long threadStartTime = System.currentTimeMillis();
        for(int i=0;i<fileNum;i++){
            String metaKey="Thread-"+this.threadOrder+"_File-"+i;
            try {
                jedis.set(metaKey,storageClient1.upload_file1(writeBuffer,null,null));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (MyException e) {
                e.printStackTrace();
            }
        }

        //TfsDemo.jedisPool.returnResource(jedis);
        //TfsDemo.defaultTfsManagerPool.add((DefaultTfsManager) tfsManager);
        long threadEndTime=System.currentTimeMillis();
        threadTime[threadOrder]=threadEndTime-threadStartTime;
        System.out.println("Thread-"+threadOrder+":"+(threadEndTime-threadStartTime)+"ms");
        countDownLatch.countDown();
        return storageClient1;
    }
}
