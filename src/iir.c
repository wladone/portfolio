#include <prototype.h> 
#include <stdio.h> 
#define DataBlockSize 160	/* dimensiunea unui bloc de date */
#define BlockLength 100       /* numarul de blocuri de date */

Word16 x[DataBlockSize],y[DataBlockSize];

Word16 w,w1,w2,w3,w4;    // adaugam pentru ca avem mai multi coeficienti a si b
//Word16 b[]={WORD16(b0/2), WORD16(b1/2), WORD16(b2/2), WORD16(b3/2), WORD16(b4/2)};
//Word16 a[]={WORD16(a1/2), WORD16(a2/2)};

//copiem coeficientii bs1 si ad din matlab

Word16 b[]={WORD16(0.0967/2), WORD16(0.2563/2), WORD16(0.3454/2), WORD16(0.2563/2), WORD16(0.0967/2)};

Word16 a[]={WORD16(0.1128/2), WORD16(0.5923 /2), WORD16(0.0804/2), WORD16(0.0361/2)};

Word32 sum;

int main()
{
short n,i;

FILE *fpx,*fpy;

fpx=fopen("x.dat","r+b");
if (!fpx)
    printf("\nNu s-a deschis");
fpy=fopen("y.dat","w+b");
if (!fpy)
    printf("\nNu s-a deschis");

for (i=0; i<BlockLength; i++)
{
	fread(x,sizeof(Word16),DataBlockSize,fpx);

	for (n=0; n<DataBlockSize; n++)
	{
		sum = L_deposit_h(shr(x[n],1));

		sum = L_msu(sum,a[1],w1);
		sum = L_msu(sum,a[2],w2);
		sum = L_msu(sum,a[3],w3); // adaugat la codul exemplu pentru a calcula z^(-3) si z^(-4) 
		sum = L_msu(sum,a[4],w4);

		w = shl(round(sum),1);
				
			       // calculam si pentru structura din dreapta a filtrului in forma directa 2

		sum = L_mult(b[0],w);
		sum = L_mac(sum,b[1],w1);
		sum = L_mac(sum,b[2],w2);    // analog 
		sum = L_mac(sum,b[3],w3);
		sum = L_mac(sum,b[4],w4);

		y[n]= shl(mac_r(sum,b[4],w4),1);
		
		w4=w3;
		w3=w2;
		w2=w1;
		w1=w;
	}

	fwrite(y,sizeof(Word16),DataBlockSize,fpy);
}

fclose(fpx);
fclose(fpy);

return(0);
}

