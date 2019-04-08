#include <iostream>
#include <iomanip>
#include <stdint.h>
#define VERSION_MAX UINT8_MAX
#define BIT(N)
void SJ();

template <unsigned int N> // N bytes ( Not Bits )
class MetaData{
  private:
  uint8_t Version_;
  uint8_t Byte[N-1];
#ifdef O_DEBUG
  uint8_t count;
#endif
  public:
  MetaData(void){
    Version_ = 0;
    for(int i = 0; i < N-1; i++){
      Byte[i] = 0;
    }
#ifdef O_DEBUG
 count = 0;
#endif
  }
//  ~MataData(void){}
  void Init(void){
    Version_ = 0;
    for(int i = 0; i < N-1; i++){
      Byte[i] = 0;
    }
#ifdef O_DEBUG
  count = 0;
#endif
  }
  //Get function
  uint8_t Version(void)const{
    return Version_;
  }
//  uint8_t VersionMax(void)const{
//    return UINT8_MAX;
//  }
  bool IsLeaf(void){
    return Byte[0] & 1;
  }
  bool Bit(int n){
    int Q = n / 8;
    int R = n & 7; // Last 3 bit( 0 ~ 7 )
    //Last bit of Byte[0] is leaf flag.
    if(!(N-2-Q))
      R++;
    return Byte[N-1-1-Q] & (uint8_t)(1 << R);
  }
  bool SplitCheck(void)const{
#ifdef INPLACE
    if((Byte[0] & ~(uint8_t)1) != 254){
      return false;
    }
    for (int i = 1; i < N-1; i++){
      if(Byte[i] != 255){
        return false;
      }
    }
#else
    uint8_t cnt = 0;
    for(int i = 0; i < N - 1; i++){
      for(int j = 0; j < 8; j++){
        if(!(i == 0 && j == 0)){
          if(j == 0)
          cnt += ((Byte[i]) & (1));
          else
          cnt += ((Byte[i]>>j) & (1));
        }
      }
    }
#ifdef O_DEBUG
    if(cnt != count){
      SJ();
    cnt = 0;
    for(int i = 0; i < N - 1; i++){
      for(int j = 0; j < 8; j++){
        if(!(i == 0 && j == 0)){
          if(j == 0)
          cnt += ((Byte[i]) & (1));
          else
          cnt += ((Byte[i]>>j) & (1));
        }
      }
    }
    SJ();
    }
#endif
//    if(cnt == NODECARD){
//      SJ();
//    }
    if(cnt >= NODECARD - 1){
      return true;
    }
    return false;
//    bool one_allow = false;
//    if((Byte[0] & ~(uint8_t)1) != 254){
//      one_allow = true;
//      int cnt = 0;
//      for(int j = 1; j < 8; j++){
//        cnt += (int)(bool)(Byte[0]&(1<<j));
//      }
//      if(cnt < 6)
//        return false;
//    }
//    for (int i = 1; i < N-1; i++){
//      if(Byte[i] != 255){
//        if(one_allow)
//          return false;
//        one_allow = true;
////        int cnt = 0;
////        for(int j = 0; j < 8; j++){
////          cnt += (int)(bool)(Byte[i]&(1<<j));
////        }
////        if(cnt < 7)
////          return false;
//      }
//    }
#endif
    return true;
  }
  bool IsFull(void)const{
    if((Byte[0] & ~(uint8_t)1) != 254){
      return false;
    }
    for (int i = 1; i < N-1; i++){
      if(Byte[i] != 255){
        return false;
      }
    }
    return true;
  }
  //Set function
  void VersionIncr(void){
    Version_++;
  }
  void VersionReset(void){
    Version_ = 0;
  }
  void Leaf(void){
    Byte[0] |= 1;
  }
  void Iter(void){
    Byte[0] &= ~(uint8_t)1;
  }
  void Reset(void){
    Byte[0] &= 1;
    for(int i = 1; i < N-1; i++){
      Byte[i] = 0;
    }
#ifdef O_DEBUG
  count = 0;
#endif
  }
  void Reset(int n){
    int Q = n / 8;
    int R = n & 7; // Last 3 bit( 0 ~ 7 )
//    std::cout << "Q, R: " << N-2-Q << ", " << R << std::endl;
    //Last bit of Byte[0] is leaf flag.
    if(!(N-2-Q))
      R++;
    Byte[N-1-1-Q] &= ~(uint8_t)(1 << R);
#ifdef O_DEBUG
  count--;
#endif
  }
  void Set(void){
    Byte[0] &= 1;
    Byte[0] |= 254;
    for(int i = 1; i < N-1; i++){
      Byte[i] = 255;
    }
#ifdef O_DEBUG
  count = (N-1) * 8 - 1;
#endif
  }
  void Set(int n){
    int Q = n / 8;
    int R = n & 7; // Last 3 bit( 0 ~ 7 )
    //Last bit of Byte[0] is leaf flag.
    if(!(N-2-Q))
      R++;
    Byte[N-1-1-Q] |= (uint8_t)(1 << R);
#ifdef O_DEBUG
  count++;
#endif
  }
  uint8_t* Addr(int nBytes){
    return &Byte[nBytes-1];
  }
  inline int Bit2Byte(int n){
    int Q = n / 8;
    return N - 2 - Q;
  }
  inline int Byte2Atomic(int byte){
    return byte / 8 * 8;
  }
  inline uint8_t* Bit2Addr(int bit){
     return Addr(Byte2Atomic(Bit2Byte(bit)));
  }

  //Utility
  void Print(void)const {
    std::cout << "Version : " << (int)Version_ << std::endl;
    if(Byte[0]&1){
      std::cout << "IsLeaf  : " << "YES" << std::endl;
    } else {
      std::cout << "IsLeaf  : " << "NO " << std::endl;
    }
    for(int i = N-2; i >= 0; i--){
      for(int j = 0; j < 8; j++){
        int P = j + i * 8;
        if(P == 7) break;
        int pos = j + (N-2-i) * 8;
//        std::cout << std::setw(5) << pos << " : ";
        if(P > 7){
          bool var = Byte[i] & (uint8_t)(1 << j);
          std::cout << (int)var << " " ;//std::endl;
        }
        if(P < 7){
          bool var = Byte[i] & (uint8_t)(1 << (j+1));
          std::cout << (int)var << " "; // std::endl;
        }
      }
    }
    std::cout << std::endl; 
  }

};
