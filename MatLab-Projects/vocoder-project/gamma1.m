function [  ] = gamma1( gama )
%UNTITLED5 Summary of this function goes here
%   Detailed explanation goes here
%gama=1.5;
s = -1:0.1:1;

[sdm,delta]=adaptive_delta_modulation(s,gama);

figure(4)
subplot(211)
plot(sdm,'*')
hold on 
plot(s,'o')
hold off
axis([0 length(s) -1 1])
legend ('Semnal cuantizare delta adaptiva','Semnal original'), grid
title ('Cuantizare delta adaptiva')
subplot(212)
plot(delta), title('Variation of delta')

end

