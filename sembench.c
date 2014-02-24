#include <sys/sem.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>


double timedelta(struct timeval *start, struct timeval *end) {
    double res = 0;
    if (end->tv_usec > start->tv_usec) {
        res = end->tv_sec - start->tv_sec;
        res += (end->tv_usec - start->tv_usec) / 1000000.0;
    } else {
        res = (end->tv_sec - 1) - start->tv_sec;
        res += (end->tv_usec + 1000000.0 - start->tv_usec) / 1000000.0;
    }
    return res;
}


double runone(int* semsets, int nsets, int nsems, int niters) {
    struct timeval start, end;
    int i;
    int j;
    struct sembuf lock, unlock;
    gettimeofday(&start, NULL);
    for (i = 0 ; i < niters; ++i) {
        int setid = random() % nsets;
        int semid = random() % nsems;
        lock.sem_num = semid;
        lock.sem_op  = 1;
        lock.sem_flg = 0;
        unlock.sem_num = semid;
        unlock.sem_op  = 1;
        unlock.sem_flg = 0;
        semop(semsets[setid], &lock, 1);
        semop(semsets[setid], &unlock, 1);
    }
    gettimeofday(&end, NULL);
    return timedelta(&start, &end);
}


int main(int argc, char **argv) {
    int nsems = 32;
    int nsets = 20;
    int niters = 1000;
    int nprocs = 2;
    char ch;
    int i;

    while ((ch = getopt(argc, argv, "n:p:s:i:")) != -1) {
        switch(ch) {
            case 'n':
                nsems = atol(optarg);
                break;
            case 's':
                nsets = atol(optarg);
                break;
            case 'p':
                nprocs = atol(optarg);
                break;
            case 'i':
                niters = atol(optarg);
                break;
            default:
                printf("wat?\n");
        }
    }

    int *semsets = malloc(sizeof(int) * nsems);
    int *pipes = malloc(sizeof(int) * nprocs * 2);

    int outstanding_children = 0;

    for (i = 0; i < nsets; ++i) {
        int semid = semget(IPC_PRIVATE, nsems, 0);
    }

    printf("spawning %d processes\n", nprocs);
    printf("running %d semop iterations per process\n", niters);
    for (i = 0; i < nprocs; ++i) {
        pipe(&pipes[i*2]);
        int rpipe = pipes[i*2];
        int wpipe = pipes[i*2 + 1];
        if (fork() == 0) {
            close(rpipe);
            srandom(getpid());
            double duration = runone(
                semsets,
                nsets,
                nsems,
                niters
            );
            write(wpipe, &duration, sizeof(double));
            close(wpipe);
            return 0;
        } else {
            outstanding_children++;
            close(wpipe);
        }
    }

    while(outstanding_children) {
        pid_t finished = wait(NULL);
        outstanding_children--;
    }

    double min_duration = 100000;
    double avg_duration = 0;
    double max_duration = 0;

    for (int i = 0; i < nprocs; ++i) {
        int rpipe = pipes[i*2];
        double duration;
        read(rpipe, &duration, sizeof(double));
        if (duration < min_duration) {
            min_duration = duration;
        }
        if (duration > max_duration) {
            max_duration = duration;
        }
        avg_duration += (duration / nprocs);
    }

    printf("average %f, min %f, max %f\n", avg_duration, min_duration, max_duration);

    return 0;
}
