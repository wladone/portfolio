function a = r2a( r ) 
% function a = r2a( r ) % % Convert Reflexion Coefficients to Predictor Coefficients 
% % Author: Markus Hauenstein 
% Date: 30.12.2001 r = r(:); 
P = length( r ); 
A = zeros(P+1,P+1); 
A(1,:) = ones(1,P+1); 
for m = 1:P, 
    for i = 1:m-1, 
        A(i+1,m+1) = A(i+1,m) - r(m) * A(m-i+1,m); 
    end
    A(m+1,m+1) = r(m); 
end
a = A(2:P+1,P+1);