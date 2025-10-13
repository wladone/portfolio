#include <prototype.h> 
#include <stdio.h> 
#include "constants.h"
#include "functions.h"



int main()
{
Word16 x[Z];
Word32 rxx[T];
int i;
FILE *fp;

fp=fopen("../x.dat","r+b");
fread(x,sizeof(Word16),Z,fp);
if (!fp)
    printf("\nNu s-a deschis x.dat");
fclose(fp);


acorr(x,rxx, P, T);
	

/*	for (i = 0; i < DataBlockSize; i++) 
      printf("Input %d %d\n",a[i],b[i]);
*/
	for(i=0; i<T;i++)
		printf("%d  ",rxx[i]);
	

fp=fopen("../result.dat","w+b");
for(i=0; i<T;i++)
	fprintf(fp,"%d ",rxx[i]);
if (!fp)
    printf("\nNu s-a deschis  result.dat");
fclose(fp);
return(0);
}
