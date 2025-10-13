function [  ] = delta( delta )
%UNTITLED4 Summary of this function goes here
%   Detailed explanation goes here
s = -1:0.1:1;
 s0=delta_modulation(s,delta);
 
figure(3)
plot(s,'*')
hold on 
plot(s0,'o')
hold off
legend ('Semnal original', 'Semnal cuantizare delta'), grid
title ('Cuantizare delta')

end

