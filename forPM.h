#include <stdint.h>
#define CPU_FREQ_MHZ (1994)
#define CACHE_LINE_SIZE 64 

extern uint64_t writeLatency=0;
extern uint64_t clflushCnt=0;

static inline void cpu_pause()
{
      __asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void)
{
      unsigned long var;
      unsigned int hi, lo;

      asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
      var = ((unsigned long long int) hi << 32) | lo;

      return var;
}

inline void mfence()
{
      asm volatile("mfence":::"memory");
}

inline void clflush(char *data, int len)
{
      volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
      mfence();
      for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
          unsigned long etsc = read_tsc() + 
                       (unsigned long)(writeLatency*CPU_FREQ_MHZ/1000);
          asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
          while (read_tsc() < etsc) cpu_pause();
          clflushCnt++;
      }
      mfence();
}
