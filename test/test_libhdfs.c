#include "hdfs.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

  
int main(int argc, char **argv)  
{  
    // ȡ���ļ�ϵͳʵ����10.17.48.236ΪNanmNode��IP��9000Ϊ�˿ڣ�hadoopΪָ���û���  
        hdfsFS fs = hdfsConnectAsUser("10.17.48.236", 9000,"hadoop");  
        if (!fs) {  
                fprintf(stderr, "connect fail\n");  
                return -1;   
        }     
    // ��д��ʽ��O_WRONLY�����ļ���  
        hdfsFile writeFile = hdfsOpenFile(fs, "/first.txt", O_WRONLY, 4096, 0, 0);   
        if (!writeFile) {  
                fprintf(stderr, "openfile fali\n");  
                return -1;   
        }     
    // ��ղŴ������ļ���д�롰hello hdfs����  
        hdfsWrite(fs, writeFile, "hello hdfs", 10);  
    // �ر��ļ����ҶϿ����ӣ�  
        hdfsCloseFile(fs, writeFile);  
        hdfsDisconnect(fs);  
        return 0;  
}  



#include <stdio.h>
#include <string.h>
#include "hdfs.h"

int main(int argc, char **argv) {

    int existence=0;

    hdfsFS fs = hdfsConnect("default", 0);
    const char* writePath = "/user/trafodion/testfile.txt";

    existence = hdfsExists(fs, writePath);
    printf("file %s exist? = %d\n", writePath, existence);

    hdfsFile writeFile = hdfsOpenFile(fs, writePath, O_WRONLY |O_CREAT, 0, 0, 0);
    if(!writeFile) {
          fprintf(stderr, "Failed to open %s for writing!\n", writePath);
          exit(-1);
    }
    char* buffer = "Hello, World!";
    tSize num_written_bytes = hdfsWrite(fs, writeFile, (void*)buffer, strlen(buffer));
    printf("total write %d bytes\n", num_written_bytes);
    if (hdfsFlush(fs, writeFile)) {
           fprintf(stderr, "Failed to 'flush' %s\n", writePath);
          exit(-1);
    }
    hdfsCloseFile(fs, writeFile);
}
~
