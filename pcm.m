function u_ = pcm( u, nb, type)
% Functia realizeaza cunatizarea semnalului u pe nb biti folosind PCM

delta = 2*2^(-nb);

switch type
    case 1
        u_ = delta*floor(u/delta);
    case 2
        u_ = delta*round(u/delta);
    case 3
        u_ = delta*ceil(u/delta);
    case 4
        u_ = delta*floor(u/delta)+delta/2;
        index = find(u_>1);
        u_(index) = 1-delta/2;
end