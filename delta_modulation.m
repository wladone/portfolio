function s0 = delta_modulation (semnal,delta)
s0=0;

for i=1:length(semnal)
    if (semnal(i)>s0(i))
        aux=s0(i)+delta;
    else
        aux=s0(i)-delta;
    end
    s0(i+1) = aux;
end


end