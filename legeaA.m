function y = legeaA(x,A,nb,type)
% Se implementeaza prelucrarea neuniforma a semnalului de la intrare, x
% folosind legea MIU

x_A = sign(x).*(1+log10(A*abs(x)))/(1+log10(A));

index = find(abs(x)<1/A);
x_A(index) = sign(x(index)).*abs(x(index))*A./(1+log10(A));

y_A = pcm(x_A,nb,type);

y = sign(y_A).*10.^(abs(y_A)*(1+log10(A))-1)/A;

index = find(abs(y_A)<1/(1+log10(A)));
y(index) = sign(y_A(index)).*abs(y_A(index))*(1+log10(A))/A;
