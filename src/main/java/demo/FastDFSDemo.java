package demo;

import Enum1.OPTYPE;
import org.csource.fastdfs.StorageClient1;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import thread.FastDfsReadTask;
import thread.FastDfsWriteTask;
import util.FastDFSUtil;
import util.RedisUtil;
import util.common;

import java.io.IOException;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.*;

public class FastDFSDemo {
    private static String USAGE= "java -jar tfs_test.jar -optype write -fileNum 100 -fileSize 1KB -threadNum 10\n" +
            "java -jar tfs_test.jar -optype read -fileNum 100 -fileSize 1KB -threadNum 10\n";
    private static OPTYPE optype=null;
    private static int fileNum;                //单线程写入文件个数
    private static long fileSize;               //写入文件大小
    private static int threadNum;               //读写文件线程数
    private static long[] threadTime;           //存储每个线程运行时间,引用前需初始化
    private static long execStartTime;          //任务开始执行时间
    private static long execEndTime;            //任务结束运行时间

    private static long execTime;               //记录任务执行时间
    private static Vector<Thread> threadVector;  //保存线程执行读写任务的线程
    private static CountDownLatch countDownLatch;//使用CountDownLatch使主线程等待辅线程全部执行完毕。

    private static JedisPool jedisPool;
    private static Queue<StorageClient1> storageClient1Pool;
    private static ExecutorService es;            //保存线程执行读写任务的线程池
    /**
     * 根据参数初始化变量。
     * @param args
     * @return
     */
    private static int init(String[] args){
        if(args.length==0){
            System.out.println(USAGE);
            System.out.println("Missing arguments!");
            return -1;
        }
        else {
            for (int i=0;i<args.length;i++){
                if (args[i].equals("-optype")) {
                    i++;
                    if (args[i] .equals("write")) {
                        optype = OPTYPE.WRITE;
                    } else if (args[i].equals("read")) {
                        optype = OPTYPE.READ;
                    } else {
                        System.err.println("Error argument optype!");
                    }
                }
                else if (args[i].equals("-fileNum")){
                    i++;
                    fileNum=Integer.valueOf(args[i]);
                }
                else if (args[i].equals("-threadNum")){
                    i++;
                    threadNum=Integer.valueOf(args[i]);
                }
                else if (args[i].equals("-fileSize")){
                    i++;
                    fileSize= common.parseSize(args[i]);
                }
            }
        }
        es= Executors.newFixedThreadPool(threadNum);
        //threadVector=new Vector<Thread>();//初始化threadVector
        countDownLatch=new CountDownLatch(threadNum);//初始化CountDownLatch
        jedisPool= RedisUtil.createJedisPool(threadNum,10,"192.168.1.149",6379);
        storageClient1Pool= FastDFSUtil.createStorageClint1Pool("C:\\code\\fast-test\\src\\main\\resources\\client.conf",threadNum);
        return 0;
    }
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        args=new String[]{"-optype", "read", "-fileNum", "10", "-fileSize", "1KB", "-threadNum", "5"};
        int ret = init(args);//读取命令行输入参数，初始化变量。
        if (ret < 0) {
            System.exit(-1);
        }
        threadTime=new long[threadNum];//根据线程个数创建线程执行时间记录数组。
        switch (optype.ordinal()){
            //如果操作类型是write。
            case (0):{
                execStartTime=System.currentTimeMillis();//记录任务开始时间
                for (int i=0;i<threadNum;i++){
                    Jedis jedis=jedisPool.getResource();
                    StorageClient1 storageClient1=storageClient1Pool.remove();
                    FastDfsWriteTask fastDfsWriteTask=new FastDfsWriteTask(countDownLatch,jedis,storageClient1,fileNum,fileSize,i,threadTime);
                    storageClient1Pool.add((StorageClient1)es.submit(fastDfsWriteTask).get());
                }
                break;
            }
            //如果操作类型是read。
            case (1):{
                execStartTime=System.currentTimeMillis();//记录任务开始时间
                for (int i=0;i<threadNum;i++){
                    Jedis jedis=jedisPool.getResource();
                    StorageClient1 storageClient1=storageClient1Pool.remove();
                    FastDfsReadTask fastDfsReadTask=new FastDfsReadTask(countDownLatch,jedis,storageClient1,fileNum,i,threadTime);
                    storageClient1Pool.add((StorageClient1)es.submit(fastDfsReadTask).get());
            }
            break;
            }
        }
        /**
         * 打印执行结果。
         */

        countDownLatch.await();
        es.shutdown();



        System.out.println("####################################################");
        execEndTime=System.currentTimeMillis();
        execTime=execEndTime-execStartTime;//任务总运行时间
        System.out.println("Total Time:"+execTime);
        System.out.println("TotalFileNum:"+(fileNum*threadNum));//读写文件总数。
        System.out.println("OPS(N/s):"+(fileNum*threadNum*1000/execTime));//每秒读写文件数。
        System.out.println("ThreadNum"+threadNum);//线程数。
        long SumOfThreadTime=0;
        for (int i=0;i<threadNum;i++){
            SumOfThreadTime+=threadTime[i];
            System.out.println(threadTime[i]);
        }
        System.out.println("SumOfThreadTime:" + SumOfThreadTime);//线程总执行时间，小于任务总执行时间。差距越大，任务并发度越高。
        System.out.println("threadNum"+threadNum);
        System.out.println("ThreadAvgTime(ms):"+(SumOfThreadTime/threadNum));//线程平均执行时间，小于任务总执行时间。差距越大，任务并发度越高。
        System.out.println("测试结束!!!");
        return;
    }
}

