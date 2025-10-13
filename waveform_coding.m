clear all;
close all;
clc;

[xx,Fs] = audioread('vocodedsound.wav');
whos xx

% figure(1);
% plot(0:1/Fs:length(xx)/Fs-1/Fs,xx);
% title('Entire signal');
% xlabel('Time [s]');
% ylabel('Amplitude');

% we select a 1 s frame from the signal
tstart = 1;
tstop = 2;
x = xx(tstart*Fs:tstop*Fs);
figure;
plot(0:1/Fs:length(x)/Fs-1/Fs,x);
title('Original Signal');
xlabel('Time [s]');
ylabel('Amplitude');

%% PCM. A-law. Miu-law
% PCM quantization number of bits
nbits=8;
nb = 8;
type = 1; %quantization type
miu = 255; 
A = 87.56;

% Quantization levels
u = -1:1/2^nbits:1;
u_ = pcm(u,nb,type);
u_miu = legeaMiu(u,miu,nb,type);
u_A = legeaA(u,A,nb,type);
t = 0:1/Fs:length(x)/Fs-1/Fs;

figure;
line([-1,1],[0,0]);
hold on
line([0,0],[-1,1]);
h1=plot(u,u_,'r');
h2=plot(u,u_miu,'g');
h3=plot(u,u_A,'k');
legend([h1,h2,h3],{'Uniform','\mu - law','A - law'});
xlabel('Original signal');
ylabel('Quantized signal');
title('Quantization steps uniform, \mu - law and A-law');
grid

% PCM quantization
x_ = pcm(x,nb,type);

% Miu quantization
x_miu = legeaMiu(x,miu,nb,type);

% A-law quantization
x_A = legeaA(x,A,nb,type);

figure;
plot(t,x);
hold on;
plot(t,x_,'r');
plot(t,x_miu,'g');
plot(t,x_A,'k');
legend('Original','Uniform','\mu - law','A - law');
xlabel('Time [x]');
ylabel('Amplitude');
title('Quantized signal uniform, \mu - law and A-law');
