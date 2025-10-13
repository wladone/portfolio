clc;
close all;

% P3 - Tipul modulatiei: MF (semnale cu modulatie in frecventa-METODA TRIUNGHIULARA) implementat cu demodulator produs 
 
A=2;         % The amplitude of the first signal
B=2.5;       % The amplitude of the second signal 
f1=2;        % The frequency of the first signal <Hz>
f2=2;        % The frequency of the second signal  <Hz>
RSZ=15;      % the type of noise affecting the channel in db
P=15;        % The Carrier Amplitude
f0=1000;     % The carrier frequency <Hz>
delta_f=4;   % Maximum frequency deviation <Hz>
Fs=10000;    % sampling frequency <Hz>

t=-5:1/Fs:5; %t=0:1/Fs:1-1/Fs;

s1=A*cos(2*pi*f1*t); %first signal
s2=B*cos(2*pi*f2*t); %second signal
s=s1+s2; % the modulating signal (sum of the 2 signals)

purt=P*cos(2*pi*f0*t); %Carrier signal
 
figure(1),plot(t,s),title('Representation of the modulating signal s'),xlabel('Time-sec'),ylabel('Amplitude-V'),grid();
figure(2),plot(t,purt),title('Representation of the carrier signal '),xlabel('Time-sec'),ylabel('Amplitude-V'),grid();
 
w=-pi:2*pi/512:pi-2*pi/512;
S=fft(s,512);                %The Fourier transform of modulating signal

figure(3),plot(w,fftshift(abs(S))),grid(),title('Representation of the The Fourier transform of modulating signal S'),xlabel('W-rad/sec'),ylabel('Amplitude-V');
 
%forming the frequency modulated signal

delta_w=2*pi*delta_f; %maximum frequency deviation

w1=2*pi*f1; %angular frequency 1
w2=2*pi*f2; %angular frequency 2

beta1=delta_w/w1; %frequency modulation index
beta2=delta_w/w2;

wp=2*pi*f0;

%semnalul modulat MF

s_modulat=purt-beta1*beta2/2*purt.*(cos((w1-w2)*t)-cos((w1+w2)*t))-P*beta1/2*(cos((wp-w1)*t)-cos((wp+w1)*t))-P*beta2/2*(cos((wp-w2)*t)-cos((wp+w2)*t)); 
figure(4),plot(t,s_modulat),title('Representation of the modulated signal MF s_ modulat'),xlabel('Time-sec'),ylabel('Amplitude-V'),grid();
 
iesire=awgn(s_modulat,RSZ);  %adding noise over the modulated signal
 figure(5),plot(t,iesire),title('Representation of the modulated signal with noise(RSZ)'),xlabel('Time-sec'),ylabel('Amplitude-V'),grid();
 
IESIRE=fft(iesire,512); %the Fourier transform of the signal from the modulator output over which we added noise

w=-pi:2*pi/512:pi-2*pi/512;

figure(6),plot(w,fftshift(abs(IESIRE))),title('Representation of the spectrum of the modulated signal with NOISE '),xlabel('W-rad/sec'),ylabel('Amplitude-V'),grid();

%demodulator of product of the MF signal(demodularea de produs a semnalului MF)

demod=iesire.*sin(2*pi*f0*t); 
DEMOD=fft(demod,512);
figure(7),plot(w,fftshift(abs(DEMOD))),title('Representation of the spectrum of the demodulated signal after passing through the demodulator'),xlabel('W-rad/sec'),ylabel('Amplitude-V'),grid();
 
p=[diff(iesire) 0]; %signal derivation
h=abs(p);           %double alternating recovery
o=h-mean(h);        %removing the continuous component
 
%the formation of the FTJ
% Wp=2*pi*max(f1,f2); %here is the pulsation (Omega passband) corresponding to 2.5Hz
% Ws=2*pi*(f0-max(f1,f2));
% [n,Wt]=buttord(Wp,Ws,1,40,'s'); %determining the order n and the cutting frequency Wt

n=1;
Wt=2*pi*max(f1,f2)/Fs;
[bs,as]=butter(n,Wt,'s');   %design an n-order analog low-pass filter with cutoff frequency Wt
[bd,ad]=impinvar(bs,as,Fs); %determining the transfer function coefficients of the desired digital filter

iesire_demod=filter(bd,ad,o);  %iesire_demod=filter(bd,ad,demod);
IESIRE_DEMOD=fft(iesire_demod,512);

 %representation of the spectrum of the filtered signal (demodulator of product)
figure(8), plot(w,fftshift(abs(IESIRE_DEMOD))),grid(),title('Representation of the spectrum of the filtered signal (demodulator of product)'),xlabel('Time-sec'),ylabel('Amplitude-V');

%representation of the transmitted signal and the signal demodulated by the emodulator of product and filtered

figure(9), subplot(2,1,1),plot(t,s,'blue'),grid(),title('Representation of the transmitted signal '),xlabel('Time-sec'),ylabel('Amplitude-V'),
subplot(2,1,2),plot(t,iesire_demod,'red'),grid(),title('Representation of the signal demodulated and filtered '),xlabel('Time-sec'),ylabel('Amplitude-V');

