function r = a2r( a ) 
% function r = a2r( a ) % % Convert Predictor Coefficients to Reflexion Coefficients
% % Author: Markus Hauenstein 
% Date: 30.12.2001 
a = a(:); P = length( a );
A = zeros(P,P); A(:,P) = a; 
r = zeros(P,1); r(P) = A(P,P); 
for m = P:-1:2, 
    for i = 1:m-1, 
        A(i,m-1) = (A(i,m) + r(m) * A(m-i,m)) / (1-r(m)*r(m)); r(m-1) = A(m-1,m-1); 
    end
end