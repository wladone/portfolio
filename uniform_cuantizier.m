function [ prag , nivele ] = uniform_cuantizier( n )
%UNTITLED Summary of this function goes here
%   Detailed explanation goes here

valori = 2^n;
nivele = -1:2/valori:1;

for i=1:2^n
    prag(i) = (nivele(i) + nivele(i+1))/2;

end

    
 


end

