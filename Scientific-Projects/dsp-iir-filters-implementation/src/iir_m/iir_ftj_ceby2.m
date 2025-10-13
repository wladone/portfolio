close all
clc

% Datele de proiectare FTJ chebysev 2 prin metoda directa avand specificatiile:

Fs = 16000;  % Frecventa de esantionare [Hz];
Fbt= 3500;   % frecventa limita in banda de trecere [Hz], dupa modificare;
Fbo= 5000;   % Frecventa de esantionare [Hz];
Rp= 1;       % Riplu max in banda de trecere [dB];
Rs=30;       %

% a) aflam ordinul si frecventa de taiere n si Wn

Wp = 2*Fbt/Fs;
Ws = 2*Fbo/Fs;

[n,Wn] = cheb2ord(Wp,Ws,Rp,Rs);

% b) aflam coeficientii functiei filtrului H(z)

[bd,ad]= cheby2(n,Rs,Wn,'low');

figure(1),freqz(bd,ad),title('Raspunsul in frecventa')                % raspunsul in frecventa

figure(2),zplane(bd,ad),title('Pozitia polilor conform cerintei')     % pozitia polilor conform cerintei


% c) scalarea coeficientilor si a functiei cu legea L1

%i) coeficientii

h=impz(bd,ad);    

k0_1=sum(abs(h));
if k0_1>1
 bs1=bd/k0_1;
end 

%ii) semnalul

h1=impz(1,ad);
k0_2=sum(abs(h1));


% Cream semnalul cerut in enunt

t=0:1/Fs:1-1/Fs;             % 1 secs @ 16kHz sample rate
x=chirp(t,0,1,Fs/2);         % Start @ DC, cross 8kHz at t=1sec
k=fix(length(x)/160);
L=k*160;

x=x/k0_2; % scalam semnalul conform calculelor anterioare

%exportul in CW

fid=fopen('..\x.dat','w','b'); % scriem valorile in fisier
fwrite(fid,x.*2^15,'int16');
fclose(fid);

fid=fopen('..\y.dat','r','b'); % citim valorile calculate in CW
y=fread(fid,L,'int16');
fclose(fid);

y=y'/(2^15);
y=y*k0_1*k0_2; % inmultim pentru a obtine rezultatul nescalat

y_ref=filter(bd,ad,x)*k0_2; % semnalul de referinta calculat in matlab

n=0:L-1;

% afisarea semnalului de intrare
figure(3),plot(n,x),grid,title('Graficul semnalului x de intrare'),ylabel('x(n)'),xlabel('t[sec]');  

% afisarea semnalului de iesire calculat in matlab
figure(4),plot(n,y_ref),grid ,title('Graficul semnalului de iesire de referinta y_{ref} calculat din matlab'),ylabel('y_{ref}(n)'),xlabel('n[axa discreta]'); 

% afisarea semnalului de iesire din CW
figure(5),plot(n,y),grid,title('Graficul semnalului y de iesire din CW'),ylabel('y(n)'),xlabel('n[axa discreta]');

% afisarea erorii a celor 2 semnale  dintre cw si matlab
figure(6),plot(n,y-y_ref),grid,title('Graficul erorii a celor 2 semnale  dintre cw si matlab') ,ylabel('y[n]-y_{ref}[n]'); 



