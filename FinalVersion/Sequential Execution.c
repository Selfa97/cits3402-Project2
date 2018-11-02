#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <omp.h>
#include <time.h>
#include <stdbool.h>

// Define global variables
#define COL_NUM 3
int file_row_1=0;// The row number of the input file
int file_row_2=0;


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
  triplet[0] = (double*)calloc(triplet_size*COL_NUM,sizeof(double));
  for(int i=1;i<triplet_size;i++){
    triplet[i]=triplet[0]+i*COL_NUM;
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
  fseek(fp,0L,SEEK_SET);
  double ** triplet;
  if(type==1){
     triplet = create_triplet(file_row_1);
  }
  else{
     triplet = create_triplet(file_row_2);
  }
  double row,col,value;
  int i=0;
  while (fgets(line,sizeof(line),fp)!=NULL){
    char dst[COL_NUM][10];
    split(dst,line," ");
    triplet[i][0]=atof(dst[0]);
    triplet[i][1]=atof(dst[1]);
    triplet[i][2]=atof(dst[2]);
    i++;
  }
  return (triplet);
}

// Do the sparse matrix multiplication
void multiply(double** A, double** B, double **C,int result_size,int A_rows){
  int B_start =0;
  int C_position =-1;
  int iden_size =0; // identifier's size
  int * identifier = (int*)calloc(result_size,sizeof(int));
  for (int i =0; i<A_rows;i++){
    for (int j=B_start; j<file_row_2;j++){
      if (A[i][1]==B[j][0]){
        int iden_num = A[i][0]*10000 + B[j][1];
        bool found_flag = false;
        for (int k=0; k<iden_size;k++){
          if(identifier[k]==iden_num){
            C[k][2]+= A[i][2]*B[j][2];
            found_flag = true;
            break;
          }
        }
        if(found_flag==false){
          C_position++;
          iden_size++;
          C[C_position][0]=A[i][0];
          C[C_position][1]=B[j][1];
          C[C_position][2]=A[i][2]*B[j][2];
          identifier[iden_size-1]= iden_num;
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

// Print the final result to a text file
void file_ouput(double** result,int result_size,char* output_file_name){
  FILE* fp = fopen(output_file_name,"w+");
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
  int result_size = file_row_1+file_row_2;
  double** result = create_triplet(result_size);

  double begin=omp_get_wtime();
  multiply(triplet1,triplet2,result,result_size,file_row_1);
  file_ouput(result,result_size,"output_sequential");
  double end = omp_get_wtime();
  printf("now time =%f seconds\n",(double)(end - begin));
  return 0;
}
