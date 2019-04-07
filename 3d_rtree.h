#include <stdio.h>
#include <malloc.h>
#include <stdlib.h>
#include <float.h>
#include <math.h>
#include "sys/time.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <queue>	// using queue C++ style
#include <stack>	// using queue C++ style
#include <cstring>
#include "forPM.h"

#ifdef SHARED
#include <shared_mutex>
#include <mutex>
#endif
#include "index.h"

#define BIG_NUM (FLT_MAX/4.0)

#define Undefined(x) ((x)->boundary[0] > (x)->boundary[NUMDIMS])
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define cacheLineSize 64
extern int NUMDATA;
extern int SEARCH;
extern int IpthreadNum;
uint64_t flipCount = 0;
uint64_t splitCount = 0;

#ifdef BREAKDOWN
double traversal_time = 0.0;
double write_time = 0.0;
struct timeval tr1,tr2;
struct timeval wr1,wr2;
#endif

// using queue C++ style
using namespace std;
queue<struct Node*> nodeQueue;
//int SEARCH;

void SJ(void){};
//struct Rect* DEMA;
int Compare(struct Rect *o, struct Rect *n);
void RTreePrint(struct Node* n);

struct Node * RTreeNewNode(int tid);
struct Rect RTreeNodeCover(struct Node *n);
int RTreePickBranch(struct Rect *r, struct Node *n);
void RTreeInitRect(struct Rect *r);
float RTreeRectVolume(struct Rect *r);
struct Rect RTreeCombineRect(struct Rect *r, struct Rect *rr);
inline int RTreeOverlap(struct Rect *r, struct Rect *s);
int RTreeContained(struct Rect *r, struct Rect *s);
struct Node* RTreeSplitNode(struct Node *n, struct Branch *b, int tid);
void RTreeSplitNode(struct Node *n, struct Branch *b, struct Node *nn, struct Node *p);

double checkFreeSpace(struct Node* n){
    uint64_t freeSpace=0;
    uint64_t nfreeSpace=0;
    int nodeCount=0;
    int nodeSeq = 0;
	nodeQueue.push(n);
	nodeSeq++;

	while(!nodeQueue.empty()){
		struct Node* t = nodeQueue.front();
		nodeQueue.pop();
        nodeCount++;
        if(!t->meta.IsLeaf()){ // This is an internal node in the tree
			for(int i=0; i<NODECARD; i++){	
				struct Branch b = t->branch[i];
				if(t->meta.Bit(i)){ 
					nodeQueue.push(b.child);
					nodeSeq++;
                    nfreeSpace++;
				}
                else
                    freeSpace++;
			}
		}else{	
		  for(int i=0; i<NODECARD; i++){
              if(!t->meta.Bit(i))
                  freeSpace++;
              else 
                  nfreeSpace++;
		  }
        }
    }

    printf("freespace: %ld, nfreeSpace: %ld, nodeCount: %d\n", freeSpace, nfreeSpace, nodeCount);
    return freeSpace*sizeof(Branch) / nodeCount*NODECARD*sizeof(Branch);

}
void RTreePrint(struct Node* n) 
{
	int nodeSeq = 0;
	nodeQueue.push(n);
	nodeSeq++;

	while(!nodeQueue.empty()){
		struct Node* t = nodeQueue.front();
		nodeQueue.pop();
        if(!t->meta.IsLeaf()){ // This is an internal node in the tree
          t->meta.Print();
		  printf("------Not leaf : [%p]-------\n", t);
	//        printf("[%d][%s] %s\n", t->version, t->isLeaf.to_string().c_str(), t->isValid.to_string().c_str());
			for(int i=0; i<NODECARD; i++){
				struct Branch b = t->branch[i];
				
				printf("%lf %lf %lf %lf %f %f\n", b.rect.boundary[0], b.rect.boundary[1], 
							    b.rect.boundary[2], b.rect.boundary[3], b.rect.boundary[4], b.rect.boundary[5]); 

				if(t->meta.Bit(i)){ 
					nodeQueue.push(b.child);
					nodeSeq++;

				}
			}
			printf("\n");
		}else{	
        //printf("------ Leaf : [%p]-------\n", t);
          t->meta.Print();
		  for(int i=0; i<NODECARD; i++){
              if(t->meta.Bit(i))
        			printf("%lf %lf %lf %lf %f %f\n", t->branch[i].rect.boundary[0], t->branch[i].rect.boundary[1],
						    t->branch[i].rect.boundary[2], t->branch[i].rect.boundary[3], t->branch[i].rect.boundary[4], t->branch[i].rect.boundary[5]); 
		  }
          printf("\n");
        }
  }
}

// Initialize one branch cell in a node.
static void RTreeInitBranch(struct Branch *b)
{
	register int i;
	RTreeInitRect(&(b->rect));
	b->child = NULL;

}

// Initialize a Node structure.
void RTreeInitNode(struct Node *n)
{
	register int i;
#ifdef SHARED
    n->mutex_ = new std::shared_mutex();
#else
    n->mut = new pthread_mutex_t;
    *n->mut = PTHREAD_MUTEX_INITIALIZER;
#endif
    n->meta.Init();
    n->meta.Leaf();
    for(int i=0; i<NODECARD; i++)
       RTreeInitBranch(&(n->branch[i])); 
}

// Initialize the new r-tree node
struct Node* RTreeNewNode(){
	register struct Node *n;

	//n = new Node;

	n = (struct Node*)memalign(cacheLineSize, sizeof(struct Node));
	RTreeInitNode(n);
	return n;
}

// Initialize the r-tree new index
struct Node * RTreeNewIndex() { 
	struct Node *x; 
	x = RTreeNewNode(); 
	/* leaf */
	return x;
} 

extern struct Node * total_root;
struct splitLog * total_log = NULL;
int log_len = 0;
void log_init(int tnum){
    total_log = new splitLog[tnum*2];
    log_len = tnum*2;
}

// R-tree search in CPU
int hostRTreeSearch(struct Node *n, struct Rect *r, int& reSplit)
{
    int hitCount = 0;
    int i;
    char version;

#ifdef SHARED
    n->mutex_->lock_shared();
#endif
    //lock-free  
    if (!n->meta.IsLeaf()) /* this is an internal node in the tree */
    {   
        version = n->meta.Version();
        std::queue<Node*> childqueue;
        while(1){
            for (i=0; i<NODECARD; i++){
                //get all overlap branches
                if (RTreeOverlap(r,&n->branch[i].rect) && n->meta.Bit(i))
                {
                    childqueue.push(n->branch[i].child);
                }
            }
            if(version != n->meta.Version() && n->meta.Version() != 0){  //child split and added branch
                //version changed, it means new branch added
                std::queue<Node*> newchild;
                std::swap(childqueue, newchild);
                version = n->meta.Version();
            }else if(version != n->meta.Version() && n->meta.Version() == 0){
                version = n->meta.Version();
                std::queue<Node*> newchild;
                std::swap(childqueue, newchild);
            }else if(version == 0 && n->meta.Version() == 0){
                //TODO: use log
                //1, Find log
                Node* s = NULL;
                bool found = false;
                for(int j = 0; j < log_len; j++){
#ifndef FULLLOG
                    if(total_log[j].child == n){
                        s = total_log[j].sibling;
                        found = true;
                        break;
                    }
#else
                    if(total_log[j].childPoint == n){
                        s = total_log[j].siblingPoint;
                        found = true;
                        break;
                    }
#endif
                }
                if(found){
                    //2, push branches
                    for (i=0; i<NODECARD; i++){
                        //get all overlap branches
                        if (RTreeOverlap(r,&s->branch[i].rect) && s->meta.Bit(i))
                        {
                            childqueue.push(s->branch[i].child);
                        }
                    }
                } else {
                    version = n->meta.Version();
                    std::queue<Node*> newchild;
                    std::swap(childqueue, newchild);
                    SJ();
                    continue;
                }
                while(!childqueue.empty()){
                    int hc = hostRTreeSearch(childqueue.front(), r, reSplit);
                    hitCount += hc;
                    childqueue.pop();
                }
                break;
            }
            else{
                while(!childqueue.empty()){
                    int hc = hostRTreeSearch(childqueue.front(), r, reSplit);
                    hitCount += hc;
                    childqueue.pop();
                }
                break;
            }
        }
    }
    else /* this is a leaf node */
    {
        for (i=0; i<NODECARD; i++){
            if(!n->meta.Bit(i)) continue;
            if(RTreeOverlap(r,&n->branch[i].rect)) // && n->isValid.test(i))
            {  
                hitCount++;
            }
        }
    }
#ifdef SHARED
    n->mutex_->unlock_shared();
#endif    
    return hitCount;
}

// Destroy Host(CPU) R-tree
void hostRTreeDestroy(struct Node *n)
{
    if (!n->meta.IsLeaf()) /* this is an internal node in the tree */
    {
        for (int i=0; i<NODECARD; i++) {
            if (n->meta.Bit(i)) {
                hostRTreeDestroy(n->branch[i].child);
            }
        }
    }

    free(n);
    n = NULL;
}

// Inserts a new data rectangle into the index structure.
// Recursively descends tree, propagates splits back up.
// Returns 0 if node was not split.  Old node updated.
// If node was split, returns 1 and sets the pointer pointed to by
// new_node to point to the new node.  Old node updated to become one of two.
// The level argument specifies the number of steps up from the leaf
// level to insert; e.g. a data rectangle goes in at level = 0.
//
thread_local int LogCur = 0;
#ifndef INPLACE
thread_local int trace = -2;
thread_local bool OnTrace = false;
#endif


static int RTreeInsertRect2(struct Rect *r,
		int dataID, struct Node *n, struct Node **new_node, struct Node *p, struct splitLog *sl)
{

	int i;
	struct Branch b;
	
    //[OSH] n_ is child holder
    struct Node *n_;
	struct Node *n2;
    struct Node *parent = p;
    struct splitLog *log = sl;
    int rt = 0;
	bool unlocked = false;
	// Still above level for insertion, go down tree recursively
	//
	if (!n->meta.IsLeaf())
	{
#ifdef SHARED
    {
        n->mutex_->lock();
        i = RTreePickBranch(r, n);

        parent=n;
#ifdef BREAKDOWN
    gettimeofday(&tr2,0); // start the stopwatch
    traversal_time += (tr2.tv_sec-tr1.tv_sec)*1000000 + (tr2.tv_usec - tr1.tv_usec);
	gettimeofday(&wr1,0);
#endif
		n->branch[i].rect = RTreeCombineRect(r, &n->branch[i].rect);
        clflush((char*)&n->branch[i], sizeof(struct Branch));
        flipCount += sizeof(struct Branch);
#ifdef BREAKDOWN
	gettimeofday(&wr2,0);
	write_time += (wr2.tv_sec-wr1.tv_sec)*1000000 + (wr2.tv_usec - wr1.tv_usec);
	gettimeofday(&tr1,0); // start the stopwatch
#endif
        if(!n->branch[i].child->meta.IsFull()){
            unlocked = true;
            n->mutex_->unlock();
        }
    }
#else
    {
        pthread_mutex_lock(n->mut);
		i = RTreePickBranch(r, n); // i is the index of split branc

        parent = n;
#ifdef BREAKDOWN
    gettimeofday(&tr2,0); // start the stopwatch
    traversal_time += (tr2.tv_sec-tr1.tv_sec)*1000000 + (tr2.tv_usec - tr1.tv_usec);
	gettimeofday(&wr1,0);
#endif
		n->branch[i].rect = RTreeCombineRect(r, &n->branch[i].rect);
        clflush((char*)&n->branch[i], sizeof(struct Branch));
        flipCount += sizeof(struct Branch);
#ifdef BREAKDOWN
	gettimeofday(&wr2,0);
	write_time += (wr2.tv_sec-wr1.tv_sec)*1000000 + (wr2.tv_usec - wr1.tv_usec);
	gettimeofday(&tr1,0); // start the stopwatch
#endif
        if(!n->branch[i].child->meta.IsFull()){
    		unlocked = true;
            pthread_mutex_unlock(n->mut);
        }
    }
#endif
    //[OSH]Store child in n_
    n_ = n->branch[i].child;
		if (!RTreeInsertRect2(r, dataID, n->branch[i].child, &n2, n, log)) // 0 
		{
			if(!unlocked){
#ifdef SHARED
              n->mutex_->unlock();
#else
	   		  pthread_mutex_unlock(n->mut);
#endif
			}
			// child was not split
			return 0;
		}
		else    // child was split
		{
#ifdef BREAKDOWN
	gettimeofday(&wr1,0);
#endif
#ifdef SHARED
       {
         if(unlocked){
             n->mutex_->lock();
         }
	   	 b.child = n2;
		 b.rect = RTreeNodeCover(n2);

#ifdef INPLACE
        rt = RTreeAddBranch(&b, n, new_node, p, log); //(4)
#else

        //Add New node
        OnTrace = true;
        trace = i;
        rt = RTreeAddBranch(&b, n, new_node, p, log);
        //Add updated CoW node
	Node* CoW_Node = RTreeNewNode();
	n_ = CoW_Node;
	memcpy(CoW_Node,n->branch[i].child,sizeof(Node));
	clflush((char*)CoW_Node, sizeof(Node));
	flipCount += sizeof(Node);
        //b.rect = RTreeCombineRect(r, &n->branch[i].rect);
        b.rect = RTreeNodeCover(n->branch[i].child);
        b.child = CoW_Node;
        if(rt == 1){
            //Split occurs.
            RTreeAddBranch(&b, *new_node, NULL, p, NULL);
            if(OnTrace){
                n->meta.Reset(i);
                clflush((char*)n->meta.Bit2Addr(i),8); 
                flipCount += 8;
            } else {
                (*new_node)->meta.Reset(trace);
                clflush((char*)(*new_node)->meta.Bit2Addr(trace),8);
                flipCount+=8;
            }
        }
        else{
            //Split does not occur
            OnTrace = true;
            trace = i;
            rt = RTreeAddBranch(&b, n, new_node, p, log);
            if(rt == 1){
                //Split occurs
                if(OnTrace){
                    n->meta.Reset(i);
                    clflush((char*)n->meta.Bit2Addr(i),8);
                    flipCount+=8;
                } else {
                    (*new_node)->meta.Reset(trace);
                    clflush((char*)(*new_node)->meta.Bit2Addr(trace),8);
                    flipCount+=8;
                }
            }else {
                //Split does not occur.
                //Invalidate i th bitmap
                n->meta.Reset(i);
                clflush((char*)n->meta.Bit2Addr(i),8);
                flipCount+=8;
            }
        }
        OnTrace = false;
        trace = -1;

#endif
        //TODO: child->VersionIncr();
        n_->meta.VersionIncr();
        clflush((char*)n_, 1);      
        flipCount += 1;
        if(log){
            //TODO: Previous Log Invalidation ( NOT current log )
            memset(&log[(LogCur == 0)], 0, sizeof(splitLog));
            clflush(&log[(LogCur == 0)], sizeof(splitLog));
            flipCount += sizeof(splitLog);
            //Move LogCur to unused log slot.
            LogCur = (LogCur == 0); // if LogCur = 0, (LogCur == 0) = 1
            // if LogCur = 1, (LogCur == 0) = 0
        }
#ifdef INPLACE
        n->branch[i].rect = RTreeNodeCover(n->branch[i].child);
        clflush((char*)&n->branch[i], sizeof(struct Branch));
        flipCount += sizeof(Branch);
#endif
        if(!(rt == 1 && n == p)){
            n->mutex_->unlock();
        }
      }
#else
      {   
          if(unlocked){
              pthread_mutex_lock(n->mut);
          }
	      b.child = n2;
		  b.rect = RTreeNodeCover(n2);
          
 #ifdef INPLACE
          rt = RTreeAddBranch(&b, n, new_node, p, log);
 #else
          //Add new node
          OnTrace = true;
          trace = i;
          rt = RTreeAddBranch(&b, n, new_node, p, log);
        //Add updated CoW node
	Node* CoW_Node = RTreeNewNode();
	n_ = CoW_Node;
	memcpy(CoW_Node,n->branch[i].child,sizeof(Node));
	clflush((char*)CoW_Node, sizeof(Node));
	flipCount += sizeof(Node);
        //b.rect = RTreeCombineRect(r, &n->branch[i].rect);
        b.rect = RTreeNodeCover(n->branch[i].child);
        b.child = CoW_Node;
          if(rt == 1){
              //Split occurs
              printf("here is strange\n");
              RTreeAddBranch(&b, *new_node, NULL, p, NULL);
              if(OnTrace){
                  n->meta.Reset(i);
                  clflush((char*)n->meta.Bit2Addr(i),8);
                  flipCount+=8;
              }else{
                  (*new_node)->meta.Reset(trace);
                  clflush((char*)(*new_node)->meta.Bit2Addr(trace),8);
                  flipCount+=8;
              }
          } 
          else {
              //Split does not occur
              OnTrace = true;
              trace = i;
              rt = RTreeAddBranch(&b, n, new_node, p, log); 
              if(rt == 1){
                  //Split Occur
                  if(OnTrace){
                      n->meta.Reset(i);
                      clflush((char*)n->meta.Bit2Addr(i),8);
                      flipCount+=8;
                  }else{
                      (*new_node)->meta.Reset(trace);
                      clflush((char*)(*new_node)->meta.Bit2Addr(trace),8);
                      flipCount+=8;
                  }
              }else{
                  //Split does not occur.
                  //Invalidate i th bitmap
                  n->meta.Reset(i);
                  clflush((char*)n->meta.Bit2Addr(i),8);
                  flipCount+=8;
              }
          }
        OnTrace = false;
        trace = -1;
            //flipCount++;
#endif
        //TODO: child->VersionIncr();
        n_->meta.VersionIncr();
        clflush((char*)n_, 1);
        flipCount += 1;
        if(log){
            //TODO: Previous Log Invalidation ( NOT current log )
            memset(&log[(LogCur == 0)], 0, sizeof(splitLog));
            clflush((char*)&log[(LogCur == 0)], sizeof(splitLog));
            flipCount += sizeof(splitLog);
            //Move LogCur to unused log slot.
            LogCur = (LogCur == 0); // if LogCur = 0, (LogCur == 0) = 1
            // if LogCur = 1, (LogCur == 0) = 0
        }
#ifdef INPLACE
        n->branch[i].rect = RTreeNodeCover(n->branch[i].child);
        clflush((char*)&n->branch[i], sizeof(struct Branch));
        flipCount += sizeof(Branch);
#endif
        if(!(rt == 1 && n == p)){
              pthread_mutex_unlock(n->mut);
        }
#endif
      }
#ifdef BREAKDOWN
	gettimeofday(&wr2,0);
	write_time += (wr2.tv_sec-wr1.tv_sec)*1000000 + (wr2.tv_usec - wr1.tv_usec);
#endif
      return rt;
	  }
    }

	// Have reached level for insertion. Add rect, split if necessary
	else
	{
#ifdef BREAKDOWN
    gettimeofday(&tr2,0); // start the stopwatch
    traversal_time += (tr2.tv_sec-tr1.tv_sec)*1000000 + (tr2.tv_usec - tr1.tv_usec);
    gettimeofday(&wr1,0); // start the stopwatch
#endif
#ifdef SHARED
      {
          n->mutex_->lock();
          b.rect = *r;
          b.child = (struct Node *) dataID;

          rt = RTreeAddBranch(&b, n, new_node, p, log);
          if(!(rt == 1 && n == p)){
              n->mutex_->unlock();
          }
      }
#else
      {      
		pthread_mutex_lock(n->mut);	
		b.rect = *r;
		b.child = (struct Node *) dataID;
		/* child field of leaves contains dataID of data record */
		rt = RTreeAddBranch(&b, n, new_node, p, log);
        if(!(rt == 1 && n == p)){
            pthread_mutex_unlock(n->mut);
        }
      }
#endif      
#ifdef BREAKDOWN
    gettimeofday(&wr2,0); // start the stopwatch
    write_time += (wr2.tv_sec-wr1.tv_sec)*1000000 + (wr2.tv_usec - wr1.tv_usec);
#endif
        return rt;
	}
}

// Insert a data rectangle into an index structure.
// RTreeInsertRect provides for splitting the root;
// returns 1 if root was split, 0 if it was not.
// The level argument specifies the number of steps up from the leaf
// level to insert; e.g. a data rectangle goes in at level = 0.
// RTreeInsertRect2 does the recursion.
//
int RTreeInsertRect(struct Rect *R, int Did, struct Node **Root, struct splitLog *sl)
{
	register struct Rect *r = R;
	register int dataID = Did;
	register struct Node **root = Root;
	register struct Node *newroot;
    register struct splitLog *log = sl;
	struct Node *newnode;
	struct Branch b;
	int result;
	
#ifdef BREAKDOWN
	gettimeofday(&tr1,0); // start the stopwatch
#endif

    if (RTreeInsertRect2(r, dataID, *root, &newnode, *root, log))  /* root split */
	{
#ifdef BREAKDOWN
	gettimeofday(&wr1,0);
#endif
		//make new root
		newroot = RTreeNewNode();  /* grow a new root, & tree taller */
        //log update
        if(log){
#ifndef FULLLOG      
                log->parent = newroot;
                clflush((char*)log, sizeof(struct splitLog));
                flipCount += sizeof(Node *);
#else
                log->parentPoint = newroot;
                log->parent = *newroot;
                clflush((char*)&log->parent, sizeof(struct Node)+META);
                flipCount += sizeof(struct Node)+META;
#endif   
        }
                     
		//make new branch which point original root
		newroot->meta.Iter();
        //flipCount++;
		b.rect = RTreeNodeCover(*root);
		b.child = *root;
		RTreeAddBranch(&b, newroot, NULL, NULL, log);

		b.rect = RTreeNodeCover(newnode);
		b.child = newnode;
		RTreeAddBranch(&b, newroot, NULL, NULL, log);
        
         clflush((char *)newroot, 32 + 2 * sizeof(Branch));
         flipCount += 8 + 2*sizeof(Branch);
         struct Node* temp = *root;
         temp->meta.VersionIncr();
         clflush((char*)temp,1);
         flipCount += 1;
         if(log){
            //TODO: Previous Log Invalidation ( NOT current log )
            memset(&log[(LogCur == 0)], 0, sizeof(splitLog));
            clflush((char*)&log[(LogCur == 0)], sizeof(splitLog)); //1
            flipCount += sizeof(splitLog);
            //Move LogCur to unused log slot.
            LogCur = (LogCur == 0); // if LogCur = 0, (LogCur == 0) = 1
            // if LogCur = 1, (LogCur == 0) = 0
         }
#ifdef SHARED
   {
       newroot->mutex_->lock();
     {
         *root = newroot;
         temp->mutex_->unlock();
     }
     newroot->mutex_->unlock();
   }
#else
   {   
      pthread_mutex_lock((newroot)->mut);
      {
        *root = newroot;
        pthread_mutex_unlock(temp->mut);
      }
      pthread_mutex_unlock((newroot)->mut); 
   }
#endif        
		result = 1;
#ifdef BREAKDOWN
	gettimeofday(&wr2,0);
	write_time += (wr2.tv_sec-wr1.tv_sec)*1000000 + (wr2.tv_usec - wr1.tv_usec);
#endif
	}
	else{
		result = 0;
	}

	return result;
}

// Find the smallest rectangle that includes all rectangles in
// branches of a node.
//
struct Rect RTreeNodeCover(struct Node *n)
{
	int i, first_time=1;
	struct Rect r;

	RTreeInitRect(&r);
	for (i = 0; i < NODECARD; i++)
	{
		if (n->meta.Bit(i)) 
		{
			if (first_time)
			{
				r = n->branch[i].rect;
				first_time = 0;
			}
			else
				r = RTreeCombineRect(&r, &(n->branch[i].rect));
		}
	}
	return r;
}



// Pick a branch.  Pick the one that will need the smallest increase
// in area to accomodate the new rectangle.  This will result in the
// least total area for the covering rectangles in the current node.
// In case of a tie, pick the one which was smaller before, to get
// the best resolution when 	int SEARCH = atoi(args[3]);searching.
//
int RTreePickBranch(struct Rect *r, struct Node *n)
{
	struct Rect *rr;
	int i = 0, first_time=1;

	float increase, bestIncr=(float)-1, area, bestArea;
	int best = -1;
	struct Rect tmp_rect;

	for (i=0; i<NODECARD; i++)
	{
		if(!n->meta.Bit(i)) continue;
		{
			rr = &n->branch[i].rect;
			area = RTreeRectVolume(rr);
			tmp_rect = RTreeCombineRect(r, rr);
			increase = RTreeRectVolume(&tmp_rect) - area;
			if(increase == 0){
				return i;
			}
			if (increase < bestIncr || first_time)
			{
				best = i;
				bestArea = area;
				bestIncr = increase;
				first_time = 0;
			}
			else if (increase == bestIncr && area < bestArea)
			{
				best = i;
				bestArea = area;
				bestIncr = increase;
			}
		}
	}
	return best;
}


// Add a branch to a node.  Split the node if necessary.
// Returns null if node not split.  Old node updated.
// Returns a pointer to a new node if node splits, sets *new_node to address of new node.
// Old node updated, becomes one of two.
//
int RTreeAddBranch(struct Branch *B, struct Node *N, struct Node **New_node, struct Node *PN, struct splitLog *sl)
{
	register struct Branch *b = B;
	register struct Node *n = N;
	register struct Node **new_node = New_node;
	register struct Node *p = PN;
	register struct splitLog *log = sl;
	register int i, j;
	
	if (!n->meta.IsFull())  /* split won't be necessary */
	{
		for (i = 0; i < NODECARD; i++)  /* find empty branch */
		{
			if (!n->meta.Bit(i))
			{
				//printf("!root\n");
				n->branch[i] = *b;
				n->meta.Set(i);
                flipCount+=1;
                clflush((char*)&n->branch[i], sizeof(struct Branch));
                flipCount += sizeof(Branch);
                n->meta.VersionIncr();
                clflush((char*)n,META);
                flipCount += META;
				break;
			}
		}
		return 0;
	}
	else
	{
        splitCount ++;
        *new_node = RTreeNewNode();   
        register struct Node *nn = *new_node; 
       
        if(log){
#ifndef FULLLOG
            log->parent = p;                                                                                           
            log->child = n;
            log->sibling = nn;
#ifdef MULTIMETA
            memcpy(&log->meta, &n->meta, sizeof(MetaData<META>));
#endif
//            printf("sizeof splitLog: %d\n", sizeof(struct splitLog));
            clflush((char*)&log[LogCur], sizeof(struct splitLog));
            flipCount += sizeof(splitLog);
#else
            log->parentPoint = p;                                                                                           
            log->childPoint = n;
            log->siblingPoint = nn;
            log->parent = *p;                                                                                           
            log->child = *n;
            log->sibling = *nn;
#ifdef MULTIMETA
            memcpy(&log->meta, &n->meta, sizeof(MetaData<META>));
#endif
//            printf("sizeof splitLog: %d\n", sizeof(struct splitLog));
            clflush((char*)&log[LogCur], sizeof(struct splitLog));
            flipCount += sizeof(splitLog);
#endif                        
        }
    
        RTreeSplitNode(n, b, nn, p);
		  
		return 1;
	}
}

/*-----------------------------------------------------------------------------
| Initialize a rectangle to have all 0 coordinates.
-----------------------------------------------------------------------------*/
void RTreeInitRect(struct Rect *r)
{
	int i;
	for (i=0; i<NUMSIDES; i++)
		r->boundary[i] = (float)0;
}



/*-----------------------------------------------------------------------------
| Calculate the n-dimensional volume of a rectangle
-----------------------------------------------------------------------------*/
float RTreeRectVolume(struct Rect *r)
{
	int i;
	float volume = (float)1;

	if (Undefined(r))
		return (float)0;

	for(i=0; i<NUMDIMS; i++)
		volume *= r->boundary[i+NUMDIMS] - r->boundary[i];

	return volume;
}



/*-----------------------------------------------------------------------------
| Combine two rectangles, make one that includes both.
----------------------------------------false-------------------------------------*/
struct Rect RTreeCombineRect(struct Rect *r, struct Rect *rr)
{
	int i, j;
	struct Rect new_rect;

	if (Undefined(r))
		return *rr;

	if (Undefined(rr))
		return *r;

	for (i = 0; i < NUMDIMS; i++)
	{
		new_rect.boundary[i] = MIN(r->boundary[i], rr->boundary[i]);
		j = i + NUMDIMS;
		new_rect.boundary[j] = MAX(r->boundary[j], rr->boundary[j]);
	}
	return new_rect;
}

int Compare(struct Rect *o, struct Rect *n)
{
	int num = 6;
	for (int i = 0; i <NUMDIMS*2; i++)
	{
		if(o->boundary[i] == n->boundary[i]){
			num--;
        }
	}
	return num;	
}

/*-----------------------------------------------------------------------------
| Decide whether two rectangles overlap.
-----------------------------------------------------------------------------*/
inline int RTreeOverlap(struct Rect *r, struct Rect *s)
{
	int i, j;

	for (i=0; i<NUMDIMS; i++)
	{
		j = i + NUMDIMS;  
		if (r->boundary[i] > s->boundary[j] ||
				s->boundary[i] > r->boundary[j])
		{
			return FALSE;
		}
	}
	return TRUE;
}


/*-----------------------------------------------------------------------------
| Decide whether rectangle r is contained in rectangle s.
-----------------------------------------------------------------------------*/
int RTreeContained(struct Rect *r, struct Rect *s)
{
	int i, j, result;

	// undefined rect is contained in any other
	//
	if (Undefined(r))
		return TRUE;

	// no rect (except an undefined one) is contained in an undef rect
	//
	if (Undefined(s))
		return FALSE;

	result = TRUE;
	for (i = 0; i < NUMDIMS; i++)
	{
		j = i + NUMDIMS;  /* index for high sides */
		result = result
			&& r->boundary[i] >= s->boundary[i]
			&& r->boundary[j] <= s->boundary[j];
	}
	return result;
}

/*-----------------------------------------------------------------------------
| Load branch buffer with branches from full node plus the extra branch.
-----------------------------------------------------------------------------*/
static void RTreeGetBranches(struct forSplit *fs, struct Node *n, struct Branch *b)
{
	int i;

	/* load the branch buffer */
	for (i=0; i<NODECARD; i++)
	{
		fs->BranchBuf[i] = n->branch[i];
	}
	fs->BranchBuf[NODECARD] = *b;
	fs->BranchCount = NODECARD + 1;

	/* calculate rect containing all in the set */
	fs->CoverSplit = fs->BranchBuf[0].rect;
	for (i=1; i<NODECARD+1; i++)
	{
		fs->CoverSplit = RTreeCombineRect(&fs->CoverSplit, &fs->BranchBuf[i].rect);
	}	
	
	//printf("2. getbranches\n");	
}


/*-----------------------------------------------------------------------------
| Put a branch in one of the groups.
-----------------------------------------------------------------------------*/
static void RTreeClassify(struct forSplit* fs, int i, int group, struct PartitionVars *p)
{
	p->partition[i] = group;
	p->taken[i] = TRUE;

	if (p->count[group] == 0)
		p->cover[group] = fs->BranchBuf[i].rect;
	else
		p->cover[group] = RTreeCombineRect(&fs->BranchBuf[i].rect,
				&p->cover[group]);
	p->area[group] = RTreeRectVolume(&p->cover[group]);
	p->count[group]++;
}

/*-----------------------------------------------------------------------------
| Initialize a PartitionVars structure.
-----------------------------------------------------------------------------*/
static void RTreePickSeeds(struct forSplit* fs, struct PartitionVars *P)
{
	register struct PartitionVars *p = P;
	register int i, dim, high;
	register struct Rect *r, *rlow, *rhigh;
	register float w, separation, bestSep;
	RectReal width[NUMDIMS];
	int leastUpper[NUMDIMS], greatestLower[NUMDIMS];
	int seed0, seed1;

	for (dim=0; dim<NUMDIMS; dim++)
	{
		high = dim + NUMDIMS;

		/* find the rectangles farthest out in each direction
		 * along this dimens */
		greatestLower[dim] = leastUpper[dim] = 0;
		for (i=1; i<NODECARD+1; i++)
		{
			r = &fs->BranchBuf[i].rect;
			if (r->boundary[dim] >
					fs->BranchBuf[greatestLower[dim]].rect.boundary[dim])
			{
				greatestLower[dim] = i;
			}
			if (r->boundary[high] <
					fs->BranchBuf[leastUpper[dim]].rect.boundary[high])
			{
				leastUpper[dim] = i;
			}
		}

		/* find width of the whole collection along this dimension */
		width[dim] = fs->CoverSplit.boundary[high] -
			fs->CoverSplit.boundary[dim];
	}

	/* pick the best separation dimension and the two seed rects */
	for (dim=0; dim<NUMDIMS; dim++)
	{
		high = dim + NUMDIMS;

		/* divisor for normalizing by width */
		if (width[dim] == 0)
			w = (RectReal)1;
		else
			w = width[dim];

		rlow = &fs->BranchBuf[leastUpper[dim]].rect;
		rhigh = &fs->BranchBuf[greatestLower[dim]].rect;
		if (dim == 0)
		{
			seed0 = leastUpper[0];
			seed1 = greatestLower[0];
			separation = bestSep =
				(rhigh->boundary[0] -
				 rlow->boundary[NUMDIMS]) / w;
		}
		else
		{
			separation =
				(rhigh->boundary[dim] -
				 rlow->boundary[dim+NUMDIMS]) / w;
			if (separation > bestSep)
			{
				seed0 = leastUpper[dim];
				seed1 = greatestLower[dim];
				bestSep = separation;
			}
		}
	}

	if (seed0 != seed1)
	{
		RTreeClassify(fs, seed0, 0, p);
		RTreeClassify(fs, seed1, 1, p);
	}
}



/*-----------------------------------------------------------------------------
| Put each rect that is not already in a group into a group.
| Process one rect at a time, using the following hierarchy of criteria.
| In case of a tie, go to the next test.
| 1) If one group already has the max number of elements that will allow
| the minimum fill for the other group, put r in the other.
| 2) Put r in the group whose cover will expand less.  This automatically
| takes care of the case where one group cover contains r.
| 3) Put r in the group whose cover will be smaller.  This takes care of the
| case where r is contained in both covers.
| 4) Put r in the group with fewer elements.
| 5) Put in group 1 (arbitrary).
|
| Also update the covers for both groups.
-----------------------------------------------------------------------------*/
static void RTreePigeonhole(struct forSplit* fs, struct PartitionVars *P)
{
	register struct PartitionVars *p = P;
	struct Rect newCover[2];
	register int i, group;
	RectReal newArea[2], increase[2];

	for (i=0; i<NODECARD+1; i++)
	{
		if (!p->taken[i])
		{
			/* if one group too full, put rect in the other */
			if (p->count[0] >= p->total - p->minfill)
		{
				RTreeClassify(fs, i, 1, p);
				continue;
			}
			else if (p->count[1] >= p->total - p->minfill)
			{
				RTreeClassify(fs, i, 0, p);
				continue;
			}

			/* find areas of the two groups' old and new covers */
			for (group=0; group<2; group++)
			{
				if (p->count[group]>0)
					newCover[group] = RTreeCombineRect(
							&fs->BranchBuf[i].rect,
							&p->cover[group]);
				else
					newCover[group] = fs->BranchBuf[i].rect;
				newArea[group] = RTreeRectVolume(
						&newCover[group]);
				increase[group] = newArea[group]-p->area[group];
			}

			/* put rect in group whose cover will expand less */
			if (increase[0] < increase[1])
				RTreeClassify(fs, i, 0, p);
			else if (increase[1] < increase[0])
				RTreeClassify(fs, i, 1, p);

			/* put rect in group that will have a smaller cover */
			else if (p->area[0] < p->area[1])
				RTreeClassify(fs, i, 0, p);
			else if (p->area[1] < p->area[0])
				RTreeClassify(fs, i, 1, p);

			/* put rect in group with fewer elements */
			else if (p->count[0] < p->count[1])
				RTreeClassify(fs, i, 0, p);
			else
				RTreeClassify(fs, i, 1, p);
		}
	}
}



/*-----------------------------------------------------------------------------
| Pick two rects from set to be the first elements of the two groups.
| Pick the two that are separated most along any dimension, or overlap least.
| Distance for separation or overlap is measured modulo the width of the
| space covered by the entire set along that dimension.
-----------------------------------------------------------------------------*/
static void RTreeInitPVars(struct PartitionVars *P, int maxrects, int minfill)
{
	register struct PartitionVars *p = P;
	register int i;

	p->count[0] = p->count[1] = 0;
	p->total = maxrects;
	p->minfill = minfill;
	for (i=0; i<maxrects; i++)
	{
		p->taken[i] = FALSE;
		p->partition[i] = -1;
	}
}

/*-----------------------------------------------------------------------------
| Method 0 for finding a partition:
| First find two seeds, one for each group, well separated.
| Then put other rects in whichever group will be smallest after addition.
-----------------------------------------------------------------------------*/
static void RTreeMethodZero(struct forSplit* fs, struct PartitionVars *p, int minfill)
{
	RTreeInitPVars(p, fs->BranchCount, minfill);
	RTreePickSeeds(fs, p);
	RTreePigeonhole(fs, p);
}

/*-----------------------------------------------------------------------------
| Copy branches from the buffer into two nodes according to the partition.
-----------------------------------------------------------------------------*/
static void RTreeLoadNodes(struct forSplit* fs, struct Node *N, struct Node *Q,
		struct PartitionVars *P)
{
	register struct Node *n = N, *q = Q;
	register struct PartitionVars *p = P;
	register int i;

	
	int newA = p->partition[NODECARD];

    if(n->meta.IsLeaf())
        q->meta.Leaf();
    else
        q->meta.Iter();
    n->meta.VersionReset();
    q->meta.VersionIncr();
    int count = 0;
	for (i=0; i<NODECARD+1; i++)
	{
		if (p->partition[i] == newA){
			//RTreeAddSplitBranch(&fs->BranchBuf[i], q, NULL, NULL);
             q->branch[count] = fs->BranchBuf[i];;
             q->meta.Set(count);
            if(i < NODECARD){
                n->meta.Reset(i);
                flipCount++;
            }
#ifndef INPLACE
            if(OnTrace && (trace == i)){
                trace = count;
                OnTrace = false;
            }
#endif
            flipCount++;
            count++;
		}
	}
//    printf("count: %d\n", count);
    clflush((char*)q, 32 + sizeof(Branch) * count); //(2)
    flipCount += 32 + sizeof(Branch) * count;
    clflush((char*)n, META); //(3)
    flipCount += META;
}

/*-----------------------------------------------------------------------------
| Split a node.
| Divides the nodes branches and the extra one between two nodes.
| Old node is one of the new ones, and one really new one is created.
-----------------------------------------------------------------------------*/
void RTreeSplitNode(struct Node *n, struct Branch *b, struct Node *nn, struct Node *pn)
{
	register struct forSplit fs;
    register struct PartitionVars *p;
	
	/* load all the branches into a buffer, initialize old node */
	RTreeGetBranches(&fs, n, b);
	/* find partition */
	p = fs.Partitions;

	/* Note: can't use MINFILL(n) below since n was cleared by GetBranches() */
	RTreeMethodZero(&fs, p, NODECARD/2 ); // !!! check this..


	/* record how good the split was for statistics */

	/* put branches from buffer in 2 nodes according to chosen partition */
//    printf("%d %d\n", n->version, nn->version);
	RTreeLoadNodes(&fs, n, nn, p);

}
