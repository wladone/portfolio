function y = legeaMiu(x,miu,nb,type)
% Se implementeaza prelucrarea neuniforma a semnalului de la intrare, x
% folosind legea MIU

x_miu = sign(x).*log(1+miu*abs(x))/log(1+miu);

y_miu = pcm(x_miu,nb,type);

y = sign(y_miu).*((1+miu).^abs(y_miu)-1)/miu;
