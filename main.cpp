#include "3d_rtree.h"

extern int flipCount;
char queryFileName[] = "taxi_mbr_list_15sec.txt";
char dataFileName[] = "taxi_mbr_list_15sec.txt";
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
	int  tid =0;
	int  hit=0;
    struct Rect* rect=NULL;
    int startDataNum=0; // start number of fraction for each thread
    int dataNum=0;      // number of data for each thread
};

struct Node * total_root = NULL;
//P Thread Insert
#ifdef CONC
struct thread_data_both{
	int  tid =0;
	int  hit=0;
    struct Rect* Irect=NULL;
    struct Rect* Srect=NULL;
    int startDataNum=0; // start number of fraction for each thread
    int dataINum=0;      // number of data for each thread
    int dataSNum=0;      // number of data for each thread
};
void* PThreadBOTH(void *arg)
{
	// Insert the R-tree boundary region
    struct Rect r;
    struct thread_data_both* td = (struct thread_data_both*) arg;
    int reSplit = 1; 
    printf("tid: %d\n", td->tid);
    printf("hit: %d\n", td->hit);
    printf("startDataNum: %d\n", td->startDataNum);
    printf("dataINum: %d\n", td->dataINum);
    printf("dataSNum: %d\n", td->dataSNum);
    
    int iCur = 0;
    int sCur = 0;
    for(int i=0; i<td->dataINum + td->dataSNum; i++){
	    if(i%10 <3){
		//TODO: 30% is insert
		RTreeInsertRect(&(td->Irect[iCur]), td->startDataNum+iCur+1, (&total_root), (&total_log[td->tid*2]));
		iCur++;
	    } else {
		//TODO: 70% is Search
		td->hit += hostRTreeSearch(total_root, &(td->Srect[sCur]), reSplit);
		sCur++;
	    }
    }

    pthread_exit(NULL);
}
#endif


void* PThreadInsert(void *arg)
{
	// Insert the R-tree boundary region
    struct Rect r;
    struct thread_data* td = (struct thread_data*) arg;
     
    for(int i=0; i<td->dataNum; i++){
        RTreeInsertRect(&(td->rect[i]), td->startDataNum+i+1, (&total_root), (&total_log[td->tid*2]));
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
	if(argc<5){
#ifndef CONC        
        printf("Usage: %s (number_of_INSERT) (number_of_SEARCH) (number_of_Insert_THREADs) (number_of_Search_THREADs) (write_Latency) (delta)\n", args[0]);
#else
        printf("Usage: %s (number_of_INSERT) (number_of_SEARCH) (number_of_total_THREADs) (write_Latency) (delta)\n", args[0]);
#endif                
	    exit(1);
	}
	
    NUMDATA = atoi(args[1]);	// Initialize the number of Data
    SEARCH = atoi(args[2]);	// Initialize the number of search Data
    IpthreadNum = atoi(args[3]);	// Initialize the number of insert Thread
#ifndef CONC    
    int SpthreadNum = atoi(args[4]);	// Initialize the number of search Thread     
    writeLatency = atoi(args[5]);	// Initialize the number of Thread 
    float delta = atof(args[6]);
    printf("INSERT: %d, SEARCH: %d, insert_thread: %d, search_thread: %d, Write_Latency: %d delta: %f\n", NUMDATA, SEARCH, IpthreadNum, SpthreadNum, writeLatency, delta);
#else
    writeLatency = atoi(args[4]);	// Initialize the write latency 
    float delta = atof(args[5]);
    printf("INSERT: %d, SEARCH: %d, total_thread: %d, Write_Latency: %d delta: %f\n", NUMDATA, SEARCH, IpthreadNum, writeLatency, delta);
#endif    
        

	printf("NODECARD=%d\n",NODECARD);

    //################ PREPARE TO SPLIT ######################

    //##################### GET DATA #########################
    FILE* fp = fopen(dataFileName, "r+b");

    if(fp==0x0){
        printf("Line %d : Insert file open error\n", __LINE__);
        exit(1);
    }

    r = new Rect[NUMDATA];
    for(int i=0; i<NUMDATA; i++){
        fscanf(fp, "%f %f %f %f %f %f", &r[i].boundary[0], &r[i].boundary[1], &r[i].boundary[2], &r[i].boundary[3], &r[i].boundary[4], &r[i].boundary[5]);
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
        fscanf(fp, "%f %f %f %f %f %f", &sr[i].boundary[0], &sr[i].boundary[1], &sr[i].boundary[2], &sr[i].boundary[3], &sr[i].boundary[4], &sr[i].boundary[5]);
    
    	sr[i].boundary[0] += delta;
    	sr[i].boundary[1] += delta;
    	sr[i].boundary[2] += delta;
    	sr[i].boundary[3] -= delta;
    	sr[i].boundary[4] -= delta;
    	sr[i].boundary[5] -= delta;
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
        RTreeInsertRect(&r[i], i+1, &total_root, NULL);
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
    printf("clflush: %d, flipCount: %d\n", clflushCnt, flipCount); 	
    fprintf(stderr, "Insertion is done.\n");
 
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
    printf("[Concurrent!!]\n");
    struct timeval t1,t2;
    double time_t2;
    int rc;
    void *status;
    uint64_t hit = 0;

    int insertThreads = IpthreadNum;
    int searchThreads = IpthreadNum;

    pthread_t threads[IpthreadNum];
    struct thread_data_both td[IpthreadNum];
    
    int in=0, se=0; 
    NUMDATA = SEARCH * 1 / 2;
    const int insertPerThread = (NUMDATA) / insertThreads;
    const int searchPerThread = SEARCH / searchThreads;
     
    gettimeofday(&t1,0); // start the stopwatch
    for(int i=0; i<IpthreadNum; i++){
            int dataS = halfData + in*insertPerThread;
            int dataE = (in == insertThreads-1)? NUMDATA-dataS : insertPerThread;
            in++;
            int searchS = se*searchPerThread;
            int searchE = (se== searchThreads-1)? SEARCH-searchS : searchPerThread; 
            se++;
	    td[i].tid = i;
	    td[i].hit = 0;
	    td[i].Irect = &r[dataS]; 
	    td[i].Srect = &r[searchS];
	    td[i].startDataNum = dataS;
	    td[i].dataINum = dataE;
	    td[i].dataSNum = searchE;
            rc = pthread_create(&threads[i], NULL, PThreadBOTH, (void *)&td[i]);
      if (rc) {
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
      }
    }
//    printf("%d %d\n", in, se);
   
    for(int i=0; i<IpthreadNum; i++){
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
    printf("throughput: %.3lf\n", (time_t2/1000)/(NUMDATA+SEARCH-halfData));

#endif
 
//    RTreePrint(total_root); 
    hostRTreeDestroy(total_root);    // -------------- end of host search ----
	return 0;
}
