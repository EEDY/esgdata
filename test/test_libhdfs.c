#include "hdfs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

  
int main(int argc, char **argv)  
{  
    // 取得文件系统实例，10.17.48.236为NanmNode的IP，9000为端口，hadoop为指定用户；  
        hdfsFS fs = hdfsConnectAsUser("10.17.48.236", 9000,"hadoop");  
        if (!fs) {  
                fprintf(stderr, "connect fail\n");  
                return -1;   
        }     
    // 以写方式（O_WRONLY）打开文件；  
        hdfsFile writeFile = hdfsOpenFile(fs, "/first.txt", O_WRONLY, 4096, 0, 0);   
        if (!writeFile) {  
                fprintf(stderr, "openfile fali\n");  
                return -1;   
        }     
    // 向刚才创立的文件中写入“hello hdfs”；  
        hdfsWrite(fs, writeFile, "hello hdfs", 10);  
    // 关闭文件并且断开连接；  
        hdfsCloseFile(fs, writeFile);  
        hdfsDisconnect(fs);  
        return 0;  
}  



#include "hdfs.h"

int main(int argc, char **argv) {

    hdfsFS fs = hdfsConnect("default", 0);
    const char* writePath = "/tmp/testfile.txt";
    hdfsFile writeFile = hdfsOpenFile(fs, writePath, O_WRONLY |O_CREAT, 0, 0, 0);
    if(!writeFile) {
          fprintf(stderr, "Failed to open %s for writing!\n", writePath);
          exit(-1);
    }
    char* buffer = "Hello, World!";
    tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)buffer, strlen(buffer)+1);
    if (hdfsFlush(fs, writeFile)) {
           fprintf(stderr, "Failed to 'flush' %s\n", writePath);
          exit(-1);
    }
    hdfsCloseFile(fs, writeFile);
}