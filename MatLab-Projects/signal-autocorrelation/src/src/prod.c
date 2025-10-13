#include <prototype.h> 

void acorr(Word16 *x, Word32 *rxx, unsigned short N, unsigned short M)

{

	short j, k;
	Word32 sum;
	for(j=0;j<M;j++)
	{
		sum=0L;
		for(k=0; k<N; k++)
		{
				sum=L_mac(sum,x[k],x[k+j]);
				
		}
		
		rxx[j] = sum;
	}

}
