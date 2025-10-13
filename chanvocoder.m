function y = chanvocoder(carrie, modul, channe, noband, winover)

% The Channel Vocoder modulates the carrier signal with the modulation signal
% channe = number of channels         (ex. 512)
% noband = number of bands (<chan) (ex. 16)
% winover = window overlap          (ex. 1/5)

if noband>channe, error('# bands must be < # channels'), end
[rc, cc]   = size(carrie); if cc>rc, carrie = carrie'; end
[rm, cm]   = size(modul);   if cm>rm, modul = modul'; end
st         = min(rc,cc);                         % stereo or mono?
if st~= min(rm,cm), error('carrier and modulator must have same number of tracks'); end
len        = min(length(carrie),length(modul)); % find shortest length
carrie    = carrie(1:len,1:st);                % shorten carrier if needed
modul      = modul(1:len,1:st);                  % shorten modulator if needed
L          = 2*channe;                             % window length/FFT length
w          = hanning(L); if st==2, w=[w w]; end  % window/ stereo window
bands      = 1:round(channe/noband):channe;         % indices for frequency bands     
bands(end) = channe;
y          = zeros(len,st);                      % output vector
ii         = 0;
while ii*L*winover+L <= len
    ind    = round([1+ii*L*winover:ii*L*winover+L]);
    FFTmod = fft( modul(ind,:) .* w );    % window & take FFT of modulator
    FFTcar = fft( carrie(ind,:) .* w );  % window & take FFT of carrier  
    syn    = zeros(channe,st);              % place for synthesized output
    for jj = 1:noband-1                  % for each frequency band
        b        = [bands(jj):bands(jj+1)-1]; % current band
        syn(b,:) = FFTcar(b,:)*diag(mean(abs(FFTmod(b,:))));
    end                                   % take product of spectra
    midval   = FFTmod(1+L/2,:).*FFTcar(1+L/2,:); % midpoint is special
    synfull  = [syn; midval; flipud( conj( syn(2:end,:) ) );]; % + and - frequencies
    timsig   = real( ifft(synfull) );     % invert back to time
    y(ind,:) = y(ind,:) + timsig;         % add back into time waveform   
    ii       = ii+1;
end
y = 0.8*y/max(max(abs(y)));               % normalize output