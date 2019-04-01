package util;

import org.csource.common.MyException;
import org.csource.fastdfs.*;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FastDFSUtil {
    public static Queue<StorageClient1> createStorageClint1Pool(String confPath,int num){
        Queue<StorageClient1> storageClient1Pool=new ConcurrentLinkedQueue<StorageClient1>();
        for (int i=0;i<num;i++){
            storageClient1Pool.add(createStorageClint1(confPath));
        }
        return storageClient1Pool;
    }


    private static StorageClient1 createStorageClint1(String confPath){
        StorageClient1 storageClient1=null;
        try {
            ClientGlobal.init(confPath);
            TrackerClient trackerClient = new TrackerClient(ClientGlobal.g_tracker_group);
            TrackerServer trackerServer = trackerClient.getConnection();
            if (trackerServer == null) {
                throw new IllegalStateException("getConnection return null");
            }

            StorageServer storageServer = trackerClient.getStoreStorage(trackerServer);
            if (storageServer == null) {
                throw new IllegalStateException("getStoreStorage return null");
            }
            storageClient1= new StorageClient1(trackerServer,storageServer);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (MyException e) {
            e.printStackTrace();
        }
        return storageClient1;
    }

}
