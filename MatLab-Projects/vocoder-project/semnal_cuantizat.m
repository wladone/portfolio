function [ quanti_signal ] = semnal_cuantizat( n )
%UNTITLED5 Summary of this function goes here
%   Detailed explanation goes here

%[semnal, Fs] = audioread('C:\Users\Andronescu\Desktop\Tema MCTA\Victorie-Psd.wav');
%semnal1 = -1:0.2:1;

%semnal2 = -1:0.2:1;
%semnal = [semnal1;semnal2];
semnal = -1:0.1:1;
%semnal = semnal(50000:50010,1:2);
[prag , nivele] = uniform_cuantizier(n);
 


for i=1:length(semnal)

    ans = cuantizeaza_valoare(semnal(i),prag,nivele);
    quanti_signal(i) = ans;
  %  ans = cuantizeaza_valoare(semnal(i,2),prag,nivele);
   % quanti_signal(i,2) = ans;



end

n = 1:length(semnal)
figure(1)
plot(n,semnal(n),'-')
hold on;
plot(n,quanti_signal(n),'o');
legend('Original signal','SUniform cuantizied signal');
title('Uniform Cuantizier');
hold off
%player = audioplayer(quanti_signal,Fs);
%play(player);
end

