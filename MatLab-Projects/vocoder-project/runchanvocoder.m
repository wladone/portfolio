
modfile = 'modulator.wav';
carfile = 'carrier.wav';
outfile = 'vocoded.wav'

[modul, sr1] = audioread(modfile);
[carrie, sr2] = audioread(carfile);
if sr1~=sr2, disp('your sampling rates dont match'); end
y = chanvocoder(carrie, modul, 512, 16, .2);
audiowrite(outfile,y,16)

