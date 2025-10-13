function [  ] = grafic_A_u_Law( A , u )
%UNTITLED3 Summary of this function goes here
%   Detailed explanation goes here

%[semnal, Fs] = audioread('C:\Users\Andronescu\Desktop\Tema MCTA\Victorie-Psd.wav');
semnal = -1:0.1:1


for i=1:length(semnal)

    ans = A_u_law_compression(semnal(i),A,0);
    semnal_A(i) = ans;
    ans = A_u_law_compression(semnal(i),u,1);
    semnal_u(i) = ans;



end

n = 1:length(semnal)
figure(2)
plot(n,semnal(n),'-')
hold on;
plot(n,semnal_A,'o');
hold on;
plot(n,semnal_u,'*');
hold off
legend('signal','A law companding', 'u law companding');
title('A/u companding law');


end

