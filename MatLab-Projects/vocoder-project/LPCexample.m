clear all
close all
clc
[x,fs]=audioread('vocodedsound.wav');
t=(1:length(x))/fs;
figure, plot(t,x);
%%extract frame
x2=x(8000:8219);
figure, plot(x2);
%a=lpc(x2,8);
%a=lpc(x2,12);
%a=lpc(x2,16);
a=lpc(x2,20);
a=quantization(a,32);
disp('those are the reflection coeffs')
ref_coefs=a2r(a)
original_coeffs=r2a(ref_coefs)

err=filter(a,1,x2);
err2=decimate(err,2);
err=interp(err2,2);

x2r=filter(1,a,err);
figure,plot(x2r);
figure, plot(x2-x2r);
%err=1.1*err;
figure,plot(err);
%%spectru semnal
Y = fft(x2);
L=length(x2);
P2 = abs(Y/L);
P1 = P2(1:L/2+1);
P1(2:end-1) = 2*P1(2:end-1);

f = fs*(0:(L/2))/L;
plot(f,10*log(P1)) 
title('Single-Sided Amplitude Spectrum of x2(n)')
xlabel('f (Hz)')
ylabel('|P1(f)|')


fvtool(1,a);
%a=a+0.01;
x2r=filter(1,a,err);
figure,plot(x2r);
figure,plot(x2-x2r);