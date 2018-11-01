#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <omp.h>
#include <time.h>
#include <stdbool.h>
#include <mpi.h>

// Global variables
#define MASTER 0 /* taskid of first task */
#define FROM_MASTER 0 /* setting a message type */
#define FROM_WORKER 1 /* setting a message type */
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
    printf("Not enough memory for a triplet!\n");
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

  int i=0;
  while (fgets(line,sizeof(line),fp)!=NULL){
    char dst[COL_NUM][10];
    split(dst,line," ");
    triplet[i][0]=atof(dst[0]);
    triplet[i][1]=atof(dst[1]);
    triplet[i][2]=atof(dst[2]);
    i++;
  }
  // printf("%f\n",triplet[0][2]);
  return (triplet);
}

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
        if(i!=A_rows-1){
          if(A[i+1][1]==B[j][0]){
            B_start = j;
          }
        }
        break;
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
  // Get triplet2
  // char command1[50] = "sort -k2n -k1n ";
  char command2[50] = "sort -k1n -k2n ";
  // char suffix1[10] = " > out1";
  char suffix2[10] = " > out2";
  // strcat(command1,argv[1]);
  strcat(command2,argv[2]);
  // strcat(command1,suffix1);
  strcat(command2,suffix2);
  // system(command1);
  system(command2);
  // double ** triplet1 = get_triplet("out1",1);
  double ** triplet2 = get_triplet("out2",2);
  // int result_size = file_row_1+file_row_2;
  // double** result = create_triplet(result_size);

  // double* temp_send = &(triplet2[0][0]);
  // double* temp_receive = &(triplet2[0][0]);
  /*------MPI part starts---------*/
  int numtasks, /* number of tasks in partition */
	 taskid, /* a task identifier */
	 numworkers, /* number of worker tasks */
	 source, /* task id of message source */
	 dest, /* task id of message destination */
	 mtype, /* message type */
	 rows, /* rows of matrix A sent to each worker */
	 averow, extra, offset, /* used to determine rows sent to each worker */
   c_size,
	 i, j, k, rc; /* misc */

  MPI_Status status;
 	MPI_Init(&argc,&argv);
 	MPI_Comm_rank(MPI_COMM_WORLD,&taskid);
 	MPI_Comm_size(MPI_COMM_WORLD,&numtasks);
 	if (numtasks < 2 ) {
 		printf("Need at least two MPI tasks. Quitting...\n");
 		MPI_Abort(MPI_COMM_WORLD, rc);
 		exit(1);
 	}

  numworkers = numtasks -1;


  if (taskid == MASTER)
	{
    /*Get triplet 1*/
    char command1[50] = "sort -k2n -k1n ";
    char suffix1[10] = " > out1";
    strcat(command1,argv[1]);
    strcat(command1,suffix1);
    system(command1);
    double ** triplet1 = get_triplet("out1",1);

    /*Create big result triplet and real result triplet*/
    int big_result_size = file_row_1*file_row_2;
    int real_result_size = file_row_1+file_row_2;
    double** big_result = create_triplet(big_result_size);
    double** real_result = create_triplet(real_result_size);


    printf("file row 1 is %d\n",file_row_1);
    printf("real_result_size is %d\n",real_result_size);
    printf("mpi_mm has started with %d tasks.\n",numtasks);
		printf("Initializing arrays...\n");

		/* Send matrix data to the worker tasks */
		averow = file_row_1/numworkers;
		extra = file_row_1%numworkers;
		offset = 0;
    printf("averow is %d,  extra is %d\n",averow,extra);
		mtype = FROM_MASTER;
		for (dest=1; dest<=numworkers; dest++)
		{
			rows = (dest <= extra) ? averow+1 : averow;
			printf("Sending %d rows to task %d offset=%d\n",rows,dest,offset);
			MPI_Send(&offset, 1, MPI_INT, dest,mtype, MPI_COMM_WORLD);
			// printf("offset done\n");
			MPI_Send(&rows, 1, MPI_INT, dest,mtype, MPI_COMM_WORLD);
			// printf("rows done\n");
      // double* temp_send = &(triplet1[offset][0]);
			MPI_Send(triplet1[offset], rows*COL_NUM, MPI_DOUBLE,dest, mtype, MPI_COMM_WORLD);
      // MPI_Send(triplet2[0],file_row_2*COL_NUM)
			// printf("temp_send done\n");
			offset = offset + rows;
		}

    /* Receive results from worker tasks*/
    mtype = FROM_WORKER;
    for(i=1;i<numworkers+1;i++){
      printf("master receive starts\n");
      source =i;
      MPI_Recv(&offset,1,MPI_INT,source,mtype,MPI_COMM_WORLD,&status);
      printf("master receives offset done\n");
      MPI_Recv(&rows,1,MPI_INT,source,mtype,MPI_COMM_WORLD,&status);
      printf("master receives rows done\n");
      MPI_Recv(&c_size,1,MPI_INT,source,mtype,MPI_COMM_WORLD,&status);
      printf("receive c_size done\n");
      double * temp_receive = &(big_result[0][0]);
      MPI_Recv(temp_receive,c_size*COL_NUM,MPI_DOUBLE,source,mtype,MPI_COMM_WORLD,&status);
      printf("Received results from task %d\n",source);
      // for (i=0;i<c_size;i++){
      //   printf("%.0f    %.0f    %f\n",result[i][0],result[i][1],result[i][2]);
      // }
    }

	}

  /**************************** worker task ************************************/
	if (taskid > MASTER)
	{
		mtype = FROM_MASTER;
		MPI_Recv(&offset, 1, MPI_INT, MASTER, mtype,MPI_COMM_WORLD, &status);
    // printf("receive offset done\n");
		MPI_Recv(&rows, 1, MPI_INT, MASTER,mtype, MPI_COMM_WORLD, &status);
    // printf("receive rows done\n");
    double**triplet_temp = create_triplet(rows);
    double* temp_receive = &(triplet_temp[0][0]);
    MPI_Recv(temp_receive, rows*COL_NUM, MPI_DOUBLE, MASTER,mtype, MPI_COMM_WORLD, &status);
    // printf("receive temp done\n");


    double** result_temp = create_triplet(rows+file_row_2);

    multiply(triplet_temp,triplet2,result_temp,rows+file_row_2,rows);

    c_size =0;
    double* result_send = (double*)calloc((rows+file_row_2)*3,sizeof(double));
    for (i=0;i<rows+file_row_2;i++){
      if(result_temp[i][0]==0){
        break;
      }
      result_send[3*i] = result_temp[i][0];
      result_send[3*i+1] = result_temp[i][1];
      result_send[3*i+2] = result_temp[i][2];
      c_size++;
    }
    printf("c_size is %d\n",c_size);

    // for (i=0;i<rows+file_row_2;i++){
    //   printf("%.0f    %.0f    %f\n",result_temp[i][0],result_temp[i][1],result_temp[i][2]);
    // }

    /* Send Results to the master */
    mtype = FROM_WORKER;
    MPI_Send(&offset,1,MPI_INT,MASTER,mtype,MPI_COMM_WORLD);
    printf("worker sends offset done\n");
    MPI_Send(&rows,1,MPI_INT,MASTER,mtype,MPI_COMM_WORLD);
    printf("worker sends rows done\n");
    MPI_Send(&c_size,1,MPI_INT,MASTER,mtype,MPI_COMM_WORLD);
    printf("worker sends c_size done\n");
    MPI_Send(result_send,c_size*COL_NUM, MPI_DOUBLE,MASTER, mtype, MPI_COMM_WORLD);
    printf("worker sends result_send done\n");
	}

  MPI_Finalize();
  return 0;
}
