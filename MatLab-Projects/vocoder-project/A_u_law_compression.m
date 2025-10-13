function [ level ] = A_u_law_compression( nivel, A_u, tip )
%prametrii de intraare : nivel reprezinta nivelul ce trebuie cunatizat
%   A_u : reprezinta parametrul corespunzator legii A sau u
% tip : tip == 0 --> legea A, tip == 1 --> legea u


if tip == 0
    for i = 1:length(nivel)
            level(i) = sign(nivel)*( 1 + log10( A_u * abs(nivel(i))))/ ( 1 + log10(A_u));
    end
else
    for i = 1:length(nivel)
        level(i) = sign(nivel) * (log(1+A_u * abs(nivel(i)))) / log(1 + A_u);
    end
end


end

