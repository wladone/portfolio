function [symbols_quant]=quantizatione(y,b)  
q=(max(y)-abs(min(y)))/(2^b-1);
symbols_quant=round(y/q)*q;