#include <stdio.h>
#include <stdlib.h>
#include <prototype.h>
/*-----------------------------------------------------------------------*

	starcore_main.c
	
	StarCore DSP C Using Project Stationery.

        COPYRIGHT (C) : Freescale Semiconductor, Inc.
		
 *-----------------------------------------------------------------------*/

#define NUM_PTS	10		/* Only use a few points for the demo, use up to 99 for the full demo */

#define HI_PASS 0		/* 0 = lo pass, 1 = hi pass */

Word16 inputData[ ] = {
	-3024, -3024, 1512, -3528, -9073, -7561, 1512,
	504, -4536, -5544, 1008, 3529, 0, 0, 3025, 6554, 8570,
	13611, 15628, 11091, 8570, 4537, 0, -3024, 504, 3529, -3528,
	-7057, -8569, -9073, -5544, -503, 4033, 7562, 8066, 5545,
	1512, 1512, -2016, -10586, -8569, -12602, -18652, -14114,
	-12098, -12602, -12602, 0, 7562, 5041, 2521, 9578, 18149,
	12099, 10587, 10587, 8066, -3528, -12098, -13610, -16131,
	-13610, -9577, -9073, -10586, -11594, -8569, -6553, -3024,
 	1512, 7562, 6050, 2017, 1512, 4537, 5545, 504, -8569, -6553,
	-1007, -1511, -1007, -3528, -12602, -15123, -10586, -4536,
	-1007, 3025, 6050, 7058, 11091, 13611, 11091, 5041, 504,
	-2520, -11090, -17643 };

/* High-pass filter coefficients */
#if HI_PASS
 Word16  Hi_FIRCoefs[37] = {
    WORD16( -0.02056169), WORD16(  0.02914328), WORD16(  0.01015440), WORD16( -0.00173519),
    WORD16( -0.00950399), WORD16( -0.01364145), WORD16( -0.01329950), WORD16( -0.00774999),
    WORD16(  0.00231302), WORD16(  0.01438023), WORD16(  0.02410028), WORD16(  0.02663066),
    WORD16(  0.01813629), WORD16( -0.00291077), WORD16( -0.03436420), WORD16( -0.07093888),
    WORD16( -0.10518994), WORD16( -0.12957858), WORD16(  0.56957156), WORD16( -0.12957858),
    WORD16( -0.10518994), WORD16( -0.07093888), WORD16( -0.03436420), WORD16( -0.00291077),
    WORD16(  0.01813629), WORD16(  0.02663066), WORD16(  0.02410028), WORD16(  0.01438023),
    WORD16(  0.00231302), WORD16( -0.00774999), WORD16( -0.01329950), WORD16( -0.01364145),
    WORD16( -0.00950399), WORD16( -0.00173519), WORD16(  0.01015440), WORD16(  0.02914328),
    WORD16( -0.02056169) };
#else
/* Low-pass filter coefficients */
 Word16  Lo_FIRCoefs[31] = {
    WORD16( -0.00146148), WORD16( -0.00302057), WORD16( -0.00535611), WORD16( -0.00789890),
    WORD16( -0.00986130), WORD16( -0.01015001), WORD16( -0.00754271), WORD16( -0.00095777),
    WORD16(  0.01023872), WORD16(  0.02595341), WORD16(  0.04522235), WORD16(  0.06623663),
    WORD16(  0.08657257), WORD16(  0.10358802), WORD16(  0.11490465), WORD16(  0.11887384),
    WORD16(  0.11490465), WORD16(  0.10358802), WORD16(  0.08657257), WORD16(  0.06623663),
    WORD16(  0.04522235), WORD16(  0.02595341), WORD16(  0.01023872), WORD16( -0.00095777),
    WORD16( -0.00754271), WORD16( -0.01015001), WORD16( -0.00986130), WORD16( -0.00789890),
    WORD16( -0.00535611), WORD16( -0.00302057), WORD16( -0.00146148) };
#endif // HI_PASS

/* FIR filter function prototype */
Word16 fir_filter(Word16 input, Word16 *coef, long n,Word16 *history);

/* Calculate number of taps based on coefficients array used. */
/* Edit this line to select the filter used. */
#if HI_PASS
short NumTaps = sizeof(Hi_FIRCoefs)/sizeof(Word16);
#else
short NumTaps = sizeof(Lo_FIRCoefs)/sizeof(Word16);
#endif

Word16 			in;     /* Current data point to filter */
Word16 			output; /* Filtered data point */
int main(void)
{
	long i;
	static Word16	histBuff[37];		/* History buffer for taps */
	printf("Welcome to CodeWarrior for StarCore DSP!\n");
	
	/* Filter the data samples */	
	for ( i = 0; i < NUM_PTS; i++ ) {
    	in = inputData[i];
		// View a lo- or hi-pass filter
#if HI_PASS	
    	output = fir_filter(in,Hi_FIRCoefs, NumTaps, histBuff); // hi pass
#else
    	output = fir_filter(in,Lo_FIRCoefs, NumTaps, histBuff); // lo pass
#endif	
		printf( "Output %d\n", output );
	}
	
   	printf( "\nSimulation Complete.\n" );
   
	return 0;

} /* end main() */

/*****************************************************************
fir_filter - Perform fir filtering sample by sample on fractional data

Requires array of filter coefficients and pointer to history.
Returns one output sample for each input sample.

Word16 fir_filter(Word16 input,Word16 *coef,int n,Word16 *history)

    Word16 input        new fixed input sample
    Word16 *coef        pointer to filter coefficients
    long n              number of coefficients in filter
    Word16 *history     history array pointer
******************************************************************* */
Word16 fir_filter(Word16 input,Word16 *coef, long n, Word16 *history)
{
	long i;
	Word16 *hist_ptr,*hist1_ptr,*coef_ptr;
	Word16 output;

    hist_ptr = history;
    hist1_ptr = hist_ptr;             /* use for history update */
    coef_ptr = coef + n - 1;          /* point to last coef */

	/* form output accumulation */
    output = mult(*hist_ptr++, (*coef_ptr--));
    
    for(i = 2 ; i < n ; i++) {
        *hist1_ptr++ = *hist_ptr;       /* update history array */         
        output += mult((*hist_ptr++), (*coef_ptr--));
    }
            
    output += mult(input, (*coef_ptr));	 /* input tap */
    *hist1_ptr = input;                  /* last history */

    return(output);
} /* end fir_filter() */


