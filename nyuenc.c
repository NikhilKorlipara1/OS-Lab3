//
//  nyuenc.c
//  nyuenc
//
//  Created by Nikhil Korlipara on 11/15/22.
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdbool.h>
#include <fcntl.h>
#include <pthread.h>

#define FOUR_K 4096

char* files[100];
int numFiles=0;
int numJobs=0;
int fileSizes[100];

static void runSequential();
static void runParallel();
static void createTasks();
static int getFileSize(char* file);
static void doWork(int taskidx);
static void* produce(void* arg);
static void* consume(void* arg);

void printUsage(char* progName) {
    printf("Usage is :\n\n\t\t%s [-j <numOfJobs>] <file1> [<file2> ....]", progName);
}

int main(int argc, char* argv[]) {
    
    for(int i=1; i<argc; i++) {
        if (strcmp("-j", argv[i]) == 0) {
            if (i+1 < argc) {
                numJobs = atoi(argv[i+1]);
                i += 1; // skip the next argument
            } else {
                printf("-j argument should be followed by number of jobs");
                printUsage(argv[0]);
                exit(-1);
            }
        } else {
            if (numFiles+1 > 100) {
                printf("number of input files cannot exceed 100");
                printUsage(argv[0]);
                exit(-1);
            }
            files[numFiles++] = argv[i];
        }
    }
    if (numFiles == 0) {
        printf("Invalid input. atleast on input file is required.");
        printUsage(argv[0]);
        exit(-1);
    }
    
    //
    if (numJobs == 0) {
        runSequential();
    } else {
      //  fprintf(stderr, "numJobs %d\n", numJobs);
        runParallel();
    }
}

static void runSequential() {
    int currChar = -1;
    int cnt = 0;
    for (int i=0; i<numFiles; i++) {
        //open file
        FILE* fd;
        if (NULL != (fd = fopen(files[i], "r"))) {
            
            int c;
            while (EOF != (c = fgetc(fd))) {
                if (currChar == -1) {
                    currChar = c;
                    cnt = 1;
                } else if (currChar == c) {
                    cnt += 1;
                } else if (currChar != c) {
                    fputc(currChar, stdout);
                    fputc(cnt, stdout);
                    currChar = c;
                    cnt = 1;
                }
            }
            fclose(fd);
            fd = NULL;
        } else {
            fprintf(stderr, "Failed to access file %s. Exiting\n", files[i]);
            exit(-1);
        }
    }
    if (currChar != -1) {
        fputc(currChar, stdout);
        fputc(cnt, stdout);
    }
}

typedef struct task {
    char* address;
    int len;
    bool completed;
    char* enc;
    int enc_len;
} task_t;

static int numTasks = 0;
static task_t* tasks = NULL;
static bool work_queue_empty = true;
static pthread_mutex_t task_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t full_cond_var = PTHREAD_COND_INITIALIZER;
static pthread_cond_t empty_cond_var = PTHREAD_COND_INITIALIZER;
static int nextTaskId=-1;

static void createTasks() {
    // compute the number of tasks needed
    int i;
    for (i=0; i<numFiles; i++) {
        // get file size
        fileSizes[i] = getFileSize(files[i]);
        int cnt = (fileSizes[i] + (FOUR_K - 1) ) / FOUR_K;
        numTasks += cnt;
   //     fprintf(stderr, "file %s, size %d, numTasks %d\n", files[i], fileSizes[i], cnt);
    }
    int msize = numTasks*sizeof(task_t);
    tasks = (task_t*) malloc(msize);
    if (tasks == NULL) {
        fprintf(stderr, "malloc failed to allocate %d bytes memory for tasks. exiting\n", msize);
        exit(-1);
    }
    
    // init tasks
    for(i=0; i<numTasks; i++) {
        tasks[i].address = NULL;
        tasks[i].len = 0;
        tasks[i].completed = false;
        tasks[i].enc = NULL;
        tasks[i].enc_len = 0;
    }
    
    int taskIndex = 0;
    for (i=0; i<numFiles; i++) {
        //open file
        int fd;
        if (-1 != (fd = open(files[i], O_RDONLY))) {
            // mmap the file
            char *address = (char*)mmap(NULL, fileSizes[i], PROT_READ, MAP_SHARED, fd, 0 );
            if (address == MAP_FAILED) {
                fprintf(stderr, "failed to mmap file %s. exiting\n", files[i]);
                exit(-1);
            }
            // create a task for each 4k chunk
            int tasksForFile = (fileSizes[i] + (FOUR_K - 1)) / FOUR_K;
            
            for (int j=0; j<tasksForFile; j++) {
                tasks[taskIndex+j].address = (char*)address;
                if (j+1 == tasksForFile) {
                    tasks[taskIndex+j].len = (fileSizes[i] - j*FOUR_K);
                } else {
                    tasks[taskIndex+j].len = FOUR_K;
                    address += FOUR_K;
                }
            }
            taskIndex += tasksForFile;
        } else {
            fprintf(stderr, "Failed to access file %s. Exiting\n", files[i]);
            exit(-1);
        }
    }
    
}



static int getFileSize(char* file) {
    struct stat sb;
    if (stat(file, &sb) == -1) {
        fprintf(stderr, "stat failed for file %s. exiting\n", file);
        exit(-1);
    }
    
    if ((sb.st_mode & S_IFMT) != S_IFREG) {
        fprintf(stderr, "file %s is not a regular file. exiting\n", file);
        exit(-1);
    }
    
    return sb.st_size;
}

typedef char consumer_id_t[32];
// 256K entries in the linked list... upto 256k entries in the linked list
// how to merge the result in main thread?
// create a condition variable for each task?
static void runParallel() {
    
    //create tasks
    createTasks();
    // create a producer
    pthread_t producer_tid;
    void *ret;
    
    if (pthread_create(&producer_tid, NULL, &produce, "producer") != 0) {
        fprintf(stderr, "pthread_create() error creating producer");
        exit(-1);
    }
    
    // create a thread pool
    pthread_t *consumer_tids = (pthread_t*)malloc(sizeof(pthread_t)*numJobs);
    consumer_id_t *consumer_ids = (consumer_id_t*)malloc(sizeof(consumer_id_t)*numJobs);
    int i;
    for (i=0; i<numJobs; i++) {
        sprintf(consumer_ids[i], "consumer %d", i);
        if (pthread_create(&consumer_tids[i], NULL, &consume, consumer_ids[i]) != 0) {
            fprintf(stderr, "pthread_create() error creating %s", consumer_ids[i]);
            exit(-1);
        }
    }
    // enqueue the chunk in the work queue
    void* retval;
    pthread_join(producer_tid, &retval);
    for (i=0; i<numJobs; i++) {
        pthread_join(consumer_tids[i], &retval);
    }
    free(consumer_tids);
    free(consumer_ids);
    
    for(i=0; i<numTasks; i++) {
        if ((i+1) < numTasks) {
            if (tasks[i].enc_len>=2 && tasks[i+1].enc_len>=2 && tasks[i].enc[tasks[i].enc_len-2] == tasks[i+1].enc[0]) {
                tasks[i+1].enc[1] += tasks[i].enc[tasks[i].enc_len-1];
                tasks[i].enc_len -= 2;
            }
        }
        int j;
        for(j=0; j<tasks[i].enc_len; j++) {
            fputc(tasks[i].enc[j], stdout);
        }
    }
}

static void* produce(void* arg) {
    while(1) {
        if (pthread_mutex_lock(&task_queue_lock) != 0) {
            fprintf(stderr, "mutex_lock in producer");
            exit(-1);
        }
        while(!work_queue_empty) {
            if (pthread_cond_wait(&empty_cond_var, &task_queue_lock) != 0) {
                fprintf(stderr, "pthread_cond_wait in producer");
                exit(-1);
            }
        }
        bool exit_thread = false;
        if (nextTaskId < numTasks + numJobs - 1) {
            nextTaskId++;
            work_queue_empty = false;
            if (pthread_cond_signal(&full_cond_var) != 0) {
                fprintf(stderr, "pthread_cond_signal in producer");
                exit(-1);
            }
           // fprintf(stderr, "inside produce. nextTaskId=%d\n",nextTaskId);
        } else {
        //    fprintf(stderr, "inside produce. exiting\n");
            exit_thread = true;
        }
        if (pthread_mutex_unlock(&task_queue_lock) != 0) {
            fprintf(stderr, "pthread_mutex_unlock in producer");
            exit(-1);
        }
        if (exit_thread) {
            pthread_exit(NULL);
            break;
        }
    }
    //fprintf(stderr, "leaving produce.\n");
    return NULL;
}

static void* consume(void* arg) {
    char* jobid = (char*)arg;
    while (1) {
        if (pthread_mutex_lock(&task_queue_lock) != 0) {
            fprintf(stderr, "mutex_lock in %s\n", jobid);
            exit(-1);
        }
        while(work_queue_empty) {
            if (pthread_cond_wait(&full_cond_var, &task_queue_lock) != 0) {
                fprintf(stderr, "pthread_cond_wait in %s\n", jobid);
                exit(-1);
            }
        }
        int taskidx = -1;
        if (nextTaskId < numTasks) {
            // consume
            taskidx = nextTaskId;
        }
        work_queue_empty = true;
        if (pthread_cond_signal(&empty_cond_var) != 0) {
            fprintf(stderr, "pthread_cond_signal in %s\n", jobid);
            exit(-1);
        }
        if (pthread_mutex_unlock(&task_queue_lock) != 0) {
            fprintf(stderr, "pthread_mutex_unlock in %s\n", jobid);
            exit(-1);
        }
        if (taskidx != -1) {
            doWork(taskidx);
        } else {
            pthread_exit(NULL);
            break;
        }
    }
    return NULL;
}


static void doWork(int taskidx) {
    char enc[FOUR_K*2];
    int enc_len = 0;
    
  //  fprintf(stderr, "inside doWork(%d). len=%d\n",taskidx, tasks[taskidx].len);
    int currChar = -1;
    int cnt = 0;
    int i;
    for (i=0; i<tasks[taskidx].len; i++) {
        char c = tasks[taskidx].address[i];
        if (currChar == -1) {
            currChar = c;
            cnt = 1;
        } else if (currChar == c) {
            cnt += 1;
        } else if (currChar != c) {
            enc[enc_len++] = currChar;
            enc[enc_len++] = cnt;
            currChar = c;
            cnt = 1;
        }
    }
    if (currChar!=-1) {
        enc[enc_len++] = currChar;
        enc[enc_len++] = cnt;
    }
    tasks[taskidx].enc = (char*) malloc(enc_len*sizeof(char));
    if (tasks[taskidx].enc == NULL) {
        fprintf(stderr, "failed to allocate memory %d for encoding. exiting\n", enc_len);
        exit(-1);
    }
    tasks[taskidx].enc_len = enc_len;
    memcpy(tasks[taskidx].enc, enc, enc_len);
    tasks[taskidx].completed = true;
    //fprintf(stderr, "leaving doWork(%d). enc_len=%d\n",taskidx, enc_len);
}

