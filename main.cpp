#include "3d_rtree.h"

extern int flipCount;
char queryFileName[] = "taxi.txt";
int NUMDATA=100;
int SEARCH=10;
int IpthreadNum=8;

void cache_drop(){
    //need to drop cache to initialize search from 0
     // Remove cache
     int size = 256*1024*1024;
     char *garbage = new char[size];
     for(int i=0;i<size;++i)
         garbage[i] = i;
     for(int i=100;i<size;++i)
         garbage[i] += garbage[i-100];
     delete[] garbage;
}

// thread data
struct thread_data{
	int  tid;
	int  hit;
    struct Rect* rect;
    int startDataNum; // start number of fraction for each thread
    int dataNum;      // number of data for each thread
};

struct Node * total_root = NULL;
//P Thread Insert
void* PThreadInsert(void *arg)
{
	// Insert the R-tree boundary region
    struct Rect r;
    struct thread_data* td = (struct thread_data*) arg;
     
    for(int i=0; i<td->dataNum; i++){
        RTreeInsertRect(&(td->rect[i]), td->startDataNum+i+1, (&total_root), 0, (&total_log[td->tid]));
        td->hit++; 
	}

    pthread_exit(NULL);
}


// P Thread Search
void* PThreadSearch(void* arg)
{
	int i;
	struct Rect r;
	struct thread_data* td = (struct thread_data*) arg;

	for(i=0;i<td->dataNum;i++){
        int reSplit = 1;
		td->hit += hostRTreeSearch(total_root, &(td->rect[i]), reSplit);
	}

	pthread_exit(NULL);
}

struct Rect *r = NULL;
struct Rect *sr = NULL;

//-----------------------------------------------------------------------
//------------------------------- MAIN ----------------------------------
//-----------------------------------------------------------------------
int main(int argc, char *args[])
{
	// Check the arguments for extra information
	if(argc<4){
#ifndef CONC        
        printf("Usage: %s (number_of_INSERT) (number_of_SEARCH) (number_of_Insert_THREADs) (number_of_Search_THREADs) (write_Latency)\n", args[0]);
#else
        printf("Usage: %s (number_of_INSERT) (number_of_SEARCH) (number_of_total_THREADs) (write_Latency)\n", args[0]);
#endif                
	    exit(1);
	}
	
    NUMDATA = atoi(args[1]);	// Initialize the number of Data
    SEARCH = atoi(args[2]);	// Initialize the number of search Data
    IpthreadNum = atoi(args[3]);	// Initialize the number of insert Thread
#ifndef CONC    
    int SpthreadNum = atoi(args[4]);	// Initialize the number of search Thread     
    writeLatency = atoi(args[5]);	// Initialize the number of Thread 
    printf("INSERT: %d, SEARCH: %d, insert_thread: %d, search_thread: %d, Write_Latency: %d\n", NUMDATA, SEARCH, IpthreadNum, SpthreadNum, writeLatency);
#else
    writeLatency = atoi(args[4]);	// Initialize the write latency 
    printf("INSERT: %d, SEARCH: %d, total_thread: %d, Write_Latency: %d\n", NUMDATA, SEARCH, IpthreadNum, writeLatency);
#endif    
        

	printf("NODECARD=%d\n",NODECARD);

    //################ PREPARE TO SPLIT ######################

    //##################### GET DATA #########################
    FILE* fp = fopen("taxi.txt", "r+b");

    if(fp==0x0){
        printf("Line %d : Insert file open error\n", __LINE__);
        exit(1);
    }

    r = new Rect[NUMDATA];
    for(int i=0; i<NUMDATA; i++){
        fscanf(fp, "%f %f %f %f %f", &r[i].boundary[0], &r[i].boundary[1], &r[i].boundary[3], &r[i].boundary[4], &r[i].boundary[5]);
        if(r[i].boundary[0] > r[i].boundary[3]){ 
            float tmp = r[i].boundary[0]; 
            r[i].boundary[0] = r[i].boundary[3];  
            r[i].boundary[3] = tmp;  
            if(r[i].boundary[1] > r[i].boundary[4]){ 
                float tmp = r[i].boundary[1]; 
                r[i].boundary[1] = r[i].boundary[4];  
                r[i].boundary[4] = tmp; 
            }
        }
        else{
            if(r[i].boundary[1] > r[i].boundary[4]){ 
                float tmp = r[i].boundary[1]; 
                r[i].boundary[1] = r[i].boundary[4];  
                r[i].boundary[4] = tmp; 
            }
        }
        r[i].boundary[2] = r[i].boundary[5];
   }
    
    if(fclose(fp) != 0) printf("Insert file close error\n");
    //########################################################
    //################ GET SEARCH DATA #######################
    fp = fopen(queryFileName, "r+b");

    if(fp==0x0){
        printf("Line %d : Insert file open error\n", __LINE__);
        exit(1);
    }

    sr = new Rect[SEARCH];
    for(int i=0; i<SEARCH; i++){
        fscanf(fp, "%f %f %f %f %f", &sr[i].boundary[0], &sr[i].boundary[1], &sr[i].boundary[3], &sr[i].boundary[4], &sr[i].boundary[5]);
        if(sr[i].boundary[0] > sr[i].boundary[3]){ 
            float tmp = sr[i].boundary[0]; 
            sr[i].boundary[0] = sr[i].boundary[3];  
            sr[i].boundary[3] = tmp;  
            if(sr[i].boundary[1] > sr[i].boundary[4]){ 
                float tmp = sr[i].boundary[1]; 
                sr[i].boundary[1] = sr[i].boundary[4];  
                sr[i].boundary[4] = tmp; 
            }
        }
        else{
            if(sr[i].boundary[1] > sr[i].boundary[4]){ 
                float tmp = sr[i].boundary[1]; 
                sr[i].boundary[1] = sr[i].boundary[4];  
                sr[i].boundary[4] = tmp; 
            }
        }

        sr[i].boundary[2] = sr[i].boundary[5];
    }
    if(fclose(fp) != 0) 
        printf("Insert file close error\n");
    //########################################################


	//################ CREATE & WARM_UP ######################
	// CREATE RTree total_root node
    int indexSize;
    uint64_t ihit=0;

	// Initialize the R-tree
    total_root = RTreeNewIndex();
    log_init(IpthreadNum);

#ifdef CONC 
    const int halfData = NUMDATA / 2;
    //warm up
    for(int i=0; i< halfData; i++){
        RTreeInsertRect(&r[i], i+1, &total_root, 0, NULL);
        ihit++;
    }
    printf("Warm up (Half of NUMDATA) end %d/%d\n", halfData, ihit);
    cache_drop();
#else
    const int insertPerThread = NUMDATA / IpthreadNum;
    const int searchPerThread = SEARCH / (SpthreadNum);
#endif
	//########################################################
#ifndef CONC 
	//################# MULTI INSERT #########################
    struct timeval it1,it2;
    double time_it1;

    printf("[Only Concurrent inserting]\n");
    int rc;
    void *status;
    pthread_t threads_i[IpthreadNum];
    struct thread_data td_i[IpthreadNum];
    
    gettimeofday(&it1,0); // start the stopwatch
    for(int i=0; i<IpthreadNum; i++){
        int dataS = i*insertPerThread;
        int dataE = (i == IpthreadNum-1)? NUMDATA-dataS : insertPerThread;
        
        td_i[i].tid = i;
        td_i[i].hit = 0;
        td_i[i].rect = &r[dataS];
        td_i[i].startDataNum = dataS;
        td_i[i].dataNum = dataE;
    
        rc = pthread_create(&threads_i[i], NULL, PThreadInsert, (void *)&td_i[i]);
        if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1); } }

    for(int i=0; i<IpthreadNum; i++){
        rc = pthread_join(threads_i[i], &status);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
        ihit += td_i[i].hit;
    }
	gettimeofday(&it2,0);
    time_it1 = (it2.tv_sec-it1.tv_sec)*1000000 + (it2.tv_usec - it1.tv_usec);
    printf("insert time (msec): %.3lf\n", time_it1/1000);
    printf("#insert: %d, PThread: %d\n", ihit, IpthreadNum);
    printf("clflush: %d, flipCount: %d\n", clflushCnt, flipCount); 	
    fprintf(stderr, "Insertion is done.\n");
 
    //RTreePrint(total_root); 
	//########################################################
//-------------------------------------------------------------------------
    //###################  SEARCH DATA #######################
    struct timeval t1,t2;
    double time_t2;
    uint64_t hit = 0;

    printf("[Only Concurrent Searching]\n");

    pthread_t threads[SpthreadNum];
    struct thread_data td[SpthreadNum];
    
    gettimeofday(&t1,0); // start the stopwatch
    for(int i=0; i<SpthreadNum; i++){
        int searchS = i*searchPerThread;
        int searchE = (i==SpthreadNum-1)? SEARCH-searchS : searchPerThread;

        td[i].tid = i;
        td[i].hit = 0;
        td[i].rect = &sr[searchS];
        td[i].dataNum = searchE;

        rc = pthread_create(&threads[i], NULL, PThreadSearch, (void *)&td[i]);
        if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
    }
    
    for(int i=0; i<SpthreadNum; i++){
        rc = pthread_join(threads[i], &status);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
        hit += td[i].hit;
    }
    gettimeofday(&t2,0);
    time_t2 = (t2.tv_sec-t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);
    printf("host search time (msec): %.3lf\n", time_t2/1000);
    printf("Host Hit counter = %ld, PThread: %d\n", hit, SpthreadNum);
    //	printf("==========================================\n");
	//########################################################
#else
    struct timeval t1,t2;
    double time_t2;
    int rc;
    void *status;
    uint64_t hit = 0;

    int temp = ((IpthreadNum % 10) > 3)? 3 : IpthreadNum%10;
    int insertThreads = ((IpthreadNum/10)*3+temp);
    int searchThreads = (IpthreadNum - insertThreads);

    pthread_t threads_i[insertThreads];
    struct thread_data td_i[insertThreads];
    pthread_t threads[searchThreads];
    struct thread_data td[searchThreads];
    gettimeofday(&t1,0); // start the stopwatch
    
    int in=0, se=0; 
    const int insertPerThread = (NUMDATA-halfData) / insertThreads;
    const int searchPerThread = SEARCH / searchThreads;
    
    for(int i=0; i<IpthreadNum; i++){
       
      switch(i%10)
      {
        case 0:
        case 1:
        case 2: {
            int dataS = halfData + in*insertPerThread;
            int dataE = (in == insertThreads-1)? NUMDATA-dataS : insertPerThread;
            in++;
             
            td_i[i].tid = i;
            td_i[i].hit = 0;
            td_i[i].rect = &r[dataS];
            td_i[i].startDataNum = dataS;
            td_i[i].dataNum = dataE;
            rc = pthread_create(&threads_i[i], NULL, PThreadInsert, (void *)&td_i[i]);
            break;
        }
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
        case 8:
        case 9: {
            int searchS = se*searchPerThread;
            int searchE = (se== searchThreads-1)? SEARCH-searchS : searchPerThread; 
            se++;
             
            td[i].tid = i;
            td[i].hit = 0;
            td[i].rect = &sr[searchS];
            td[i].dataNum = searchE;
            rc = pthread_create(&threads[i], NULL, PThreadSearch, (void *)&td[i]);
            break;
        }
        default:
            return 0;
      }
    }
    printf("%d %d\n", in, se);
   
    for(int i=0; i<insertThreads; i++){
        rc = pthread_join(threads_i[i], &status);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
       ihit += td_i[i].hit;
    }
    for(int i=0; i<searchThreads; i++){
        rc = pthread_join(threads[i], &status);
        if (rc) {
            printf("ERROR; return code from pthread_join() is %d\n", rc);
            exit(-1);
        }
       hit += td[i].hit;
    }
    
    gettimeofday(&t2,0);
    time_t2 = (t2.tv_sec-t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);
    printf("concurrent time (msec): %.3lf\n", time_t2/1000);
    printf("Host Hit counter = %ld, InsertCounter: %d\n", hit, ihit);

#endif
 
    hostRTreeDestroy(total_root);    // -------------- end of host search ----
	return 0;
}
