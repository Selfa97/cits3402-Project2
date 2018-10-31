#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <omp.h>
#include <time.h>
#include <stdbool.h>

#define NUM_THREADS 4

// Global variables
int file_row_1=0;// The row number of the input file
int file_row_2=0;
int size =0; // matrix size

// Split each line and store the result in a matrix
void split(char dst[][10], char* str, const char* spl){
  int n=0;
  char *result = NULL;
  result = strtok(str, spl);
  while( result != NULL ){
    strcpy(dst[n++], result);
    result = strtok(NULL, spl);
  }
}

// Initialize an empty triplet
double ** create_triplet(int triplet_size){
  double ** triplet;
  triplet = (double**)calloc(triplet_size,sizeof(double*));
  for (int i=0;i<triplet_size;i++){
    triplet[i] = (double*)calloc(3,sizeof(double));
  }
  if(triplet==NULL){
    printf("Not enough memory!\n");
    exit(EXIT_FAILURE);
  }
  return (triplet);
}

// Read the sparse matrix file and get the complete matrix
double ** get_triplet(char *file_name,int type){
  FILE *fp = fopen(file_name,"r");
  if(fp==NULL){
    printf("Opening target file %s failed!",file_name);
    exit(EXIT_FAILURE);
  }
  char * last_line;
  char line[BUFSIZ];

  if(type==1){
    while(!feof(fp)){
      fgets(line,sizeof(line),fp);
      if(feof(fp))
        last_line =line;
      file_row_1++;
    }
    file_row_1--; // get the correct file row number
  }
  else{
    while(!feof(fp)){
      fgets(line,sizeof(line),fp);
      if(feof(fp))
        last_line =line;
      file_row_2++;
    }
    file_row_2--; // get the correct file row number
  }

  size =atoi(strtok(last_line," "));

  fseek(fp,0L,SEEK_SET);
  double ** triplet;
  if(type==1){
     triplet = create_triplet(file_row_1);
  }
  else{
     triplet = create_triplet(file_row_2);
  }
  int i=0;
  while (fgets(line,sizeof(line),fp)!=NULL){
    char dst[3][10];
    split(dst,line," ");
    triplet[i][0]=atof(dst[0]);
    triplet[i][1]=atof(dst[1]);
    triplet[i][2]=atof(dst[2]);
    i++;
  }
  // printf("%f\n",triplet[0][2]);
  return (triplet);
}

void multiply(double** A, double** B, double **C,int result_size){
  double*** local_C = (double***)calloc(NUM_THREADS,sizeof(double**));
  int** local_identifiers = (int**)calloc(NUM_THREADS,sizeof(int*));
  int* iden_size = (int*)calloc(NUM_THREADS,sizeof(int));

  for (int i = 0; i < NUM_THREADS; i++) {
    local_identifiers[i] = (int*)calloc(result_size/(NUM_THREADS-1),sizeof(int));
  }

  #pragma omp parallel
  {
    int thr = omp_get_thread_num();
    local_C[thr] = create_triplet(result_size/(NUM_THREADS-1));
    iden_size[thr] = 0;
    int* identifier = local_identifiers[thr];
    int B_start =0;
    int C_position =-1;
    #pragma omp for schedule(auto)
    for (int i =0; i<file_row_1;i++){
      for (int j=B_start; j<file_row_2;j++){
        if (A[i][1]==B[j][0]){
          int iden_num = A[i][0]*10000 + B[j][1];
          // printf("A[i][0] is %f\n",A[i][0]);
          // printf("A[i][1] is %f\n",A[i][1]);
          // printf("B[j][0] is %f\n",B[j][0]);
          // printf("B[j][1] is %f\n",B[j][1]);
          // printf("iden_num is %d\n",iden_num);
          bool found_flag = false;
          for (int k=0; k<iden_size[thr];k++){
            if(identifier[k]==iden_num){
              // printf("identifier[k] is %d\n",identifier[k]);
              local_C[thr][k][2]+= A[i][2]*B[j][2];
              // printf("C[k][2] is %f\n",C[k][2]);
              found_flag = true;
              break;
            }
          }
          if(found_flag==false){
            C_position++;
            iden_size[thr]++;
            // printf("C_position is %d\n",C_position);
            local_C[thr][C_position][0]=A[i][0];
            // printf("C[C_position][0] is %f\n",C[C_position][0]);
            local_C[thr][C_position][1]=B[j][1];
            // printf("C[C_position][1] is %f\n",C[C_position][1]);
            local_C[thr][C_position][2]=A[i][2]*B[j][2];
            // printf("C[C_position][2] is %f\n",C[C_position][2]);
            identifier[iden_size[thr]-1]= iden_num;
            // printf("identifier[iden_size-1] is %d\n",identifier[iden_size-1]);
            // printf("i is %d\n",i);
            // printf("j is %d\n",j);
          }
        }
        if(A[i][1]<B[j][0]){
          if(i!=file_row_1-1){
            if(A[i+1][1]==B[j][0]){
              B_start = j;
            }
          }
          break;
        }
      }
    }
  }

  int* final_identifiers = (int*)calloc(result_size,sizeof(int));
  int C_position = -1;

  for(int i = 0; i < iden_size[0]; i++) {
    C_position++;
    C[C_position][0] = local_C[0][i][0];
    C[C_position][1] = local_C[0][i][1];
    C[C_position][2] = local_C[0][i][2];
    final_identifiers[C_position] = local_identifiers[0][i];
  }


  for(int i = 1; i < NUM_THREADS; i++) {
    for(int j = 0; j < iden_size[i]; j++) {
      bool found_flag = false;
      #pragma omp parallel for schedule(auto)
      for(int k = 0; k < C_position+1; k++) {
        if(!found_flag && local_identifiers[i][j] == final_identifiers[k]) {
          C[k][2] += local_C[i][j][2];
          found_flag = true;
        }
      }
      if(!found_flag) {
        C_position++;
        C[C_position][0] = local_C[i][j][0];
        C[C_position][1] = local_C[i][j][1];
        C[C_position][2] = local_C[i][j][2];
        final_identifiers[C_position] = local_identifiers[i][j];
      }
    }
  }



}

void file_ouput(double** result,int result_size){
  FILE* fp = fopen("output","w+");
  for (int i=0;i<result_size;i++){
    if(result[i][0]==0){
      break;
    }
    else{
      fprintf(fp,"%.0f %.0f %f\n",result[i][0],result[i][1],result[i][2]);
    }
  }
}


int main(int argc, char *argv[]){
  omp_set_num_threads(NUM_THREADS);

  char command1[50] = "sort -k2n -k1n ";
  char command2[50] = "sort -k1n -k2n ";
  char suffix1[10] = " > out1";
  char suffix2[10] = " > out2";
  strcat(command1,argv[1]);
  strcat(command2,argv[2]);
  strcat(command1,suffix1);
  strcat(command2,suffix2);
  system(command1);
  system(command2);
  double ** triplet1 = get_triplet("out1",1);
  double ** triplet2 = get_triplet("out2",2);
  // int result_size = (file_row_1>file_row_2)?file_row_1:file_row_2;
  int result_size = file_row_1+file_row_2;
  double** result = create_triplet(result_size);
  // printf("%d   %d\n",file_row_1,file_row_2);
  // printf("result size is %d\n",result_size);

  double begin=omp_get_wtime();
  multiply(triplet1,triplet2,result,result_size);
  double end = omp_get_wtime();
  printf("now time =%f\n",(double)(end - begin));
  file_ouput(result,result_size);

  return 0;
}
