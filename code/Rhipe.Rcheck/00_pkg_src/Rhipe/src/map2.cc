// #include "map2.hh"

// MapReader::MapReader(int32_t bytesbuffer, SEXP runcode){
//     storecollector = (char*)malloc(bytesbuffer+2*bytesbuffer);
//     _position = 0;
//     _numsaved = 0;
//     code2run = runcode;
//   }
  
// MapReader::~MapReader(){
//     if(storecollector) free(storecollector);
// }
// bool MapReader::readObject(int32_t n){
//   int vo;
//   if((vo=fread(storecollector+_position,n,1,CMMNC->BSTDIN))<=0){
//     return(false);
//   }
//   _sizes.push_back(vo);
//   _position+= vo;
//   _numsaved ++;
//   return(true);
// }
// bool MapReader::spill(){
//   return _position>0? true:false;
// }
// void MapReader::run_map_code(){
//   _position = 0;
//   _numsaved = 0;
//   int Rerr;
//   SEXP kvector,vvector;
//   PROTECT(kvector = Rf_allocVector(VECSXP,_numsaved));
//   PROTECT(vvector = Rf_allocVector(VECSXP,_numsaved));
//   int32_t offset = 0;
//   for(int i=0; i < _numsaved;i+=2){
//     SET_VECTOR_ELT(kvector, i, readFromMem(storecollector+offset,_sizes[i]));
//     offset+=_sizes[i];
//     SET_VECTOR_ELT(vvector, i, readFromMem(storecollector+offset,_sizes[i+1]));
//     offset+=_sizes[i+1]
//       }
//   Rf_defineVar(Rf_install("map.keys"),kvector,R_GlobalEnv);
//   Rf_defineVar(Rf_install("map.values"),vvector,R_GlobalEnv);
//   WRAP_R_EVAL(code2run,NULL,&Rerr);
// }

