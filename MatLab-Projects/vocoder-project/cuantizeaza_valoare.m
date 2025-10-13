function [ val_cuantizata ] = cuantizeaza_valoare( valoare , praguri , nivele )
%UNTITLED3 Summary of this function goes here
%   Detailed explanation goes here
praguri
nivele
val_cuantizata = 0;
if (valoare < nivele(1))  val_cuantizata = praguri(1);
else if (valoare >= nivele(length(nivele)))  val_cuantizata = praguri(length(praguri));
    
    else  for i=1:length(nivele)-1
            if (nivele(i)<= valoare && nivele(i+1) > valoare) val_cuantizata = (nivele(i) + nivele(i+1))/2;

            end 
          end
    end
end
   
end

