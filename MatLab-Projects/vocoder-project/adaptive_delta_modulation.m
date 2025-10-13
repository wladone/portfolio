function [s0,delta] = adaptive_delta_modulation (semnal, gama)
delta(1)=0.2;
s0(1)=0;
x(1)=1;
    for i=2:length(semnal)
        err=sign(semnal(i)-s0(i-1));
        x=[x err];
        delta(i)=delta(i-1)*gama^(x(i)*x(i-1));
        aux=s0(i-1)+x(i)*delta(i);
        s0=[s0, aux];
    end
end