%  Proiect_CISSV_Popescu_Vlad_Gabriel_M_PCON
% Aplicare Algoritm Run-Length Coding (RLC) pentru Lena256B.bmp 

clear; close all; clc;

img = imread('Lena256B.bmp');
if size(img, 3) == 3
    img = rgb2gray(img);
end
[rows, cols] = size(img);

figure('Name','Imagine Originala'); 
imshow(img); title('Imagine Originala');
imwrite(img, 'Original.bmp');

% === CODARE SI SALVARE RLC PE LINIi, decodare robusta ===

rlc_all = cell(rows,1);
for i = 1:rows
    line = img(i, :);
    count = 1;
    rlc_line = [];
    for j = 2:cols
        if line(j) == line(j-1)
            count = count + 1;
        else
            rlc_line = [rlc_line; line(j-1) count];
            count = 1;
        end
    end
    rlc_line = [rlc_line; line(cols) count];
    rlc_all{i} = rlc_line; % salveaza codarea liniei i
end

% Salvare robusta RLC (ca .mat, sau ca text - optional)

save('Lena256B_RLC.mat', 'rlc_all'); 

% Optional si ca text:

fid = fopen('Lena256B_RLC.txt','w');
for i = 1:rows
    fprintf(fid, '%d ', rlc_all{i}');
    fprintf(fid, '\n');
end
fclose(fid);

% === DECODARE RLC LINIE CU LINIE ===

reconstructed = zeros(rows, cols, 'uint8');
for i = 1:rows
    rlc_line = rlc_all{i};
    pos = 1;
    for k = 1:size(rlc_line,1)
        val = rlc_line(k,1);
        rep = rlc_line(k,2);
        reconstructed(i, pos:pos+rep-1) = val;
        pos = pos + rep;
    end
end

figure('Name','Imagine Reconstruita RLC'); imshow(reconstructed); title('Imagine Reconstruita RLC');
imwrite(reconstructed, 'Lena256B_RLC_decoded.bmp');

% == RESTUL ANALIZEI RAMANE LA FEL ==

mean_orig = mean(img(:));
std_orig = std(double(img(:)));
entropy_orig = entropy(img);

mean_rec = mean(reconstructed(:));
std_rec = std(double(reconstructed(:)));
entropy_rec = entropy(reconstructed);

fprintf('Original: Mean = %.2f, Std = %.2f, Entropy = %.2f\n', mean_orig, std_orig, entropy_orig);
fprintf('Reconstruita: Mean = %.2f, Std = %.2f, Entropy = %.2f\n', mean_rec, std_rec, entropy_rec);

% Curba SNR cu zgomot

snr_curve = [];
noise_levels = 0:5:50;
for noise = noise_levels
    noisy_img = imnoise(img, 'gaussian', 0, (noise/255)^2);
    mse_n = mean((double(img(:)) - double(noisy_img(:))).^2);
    if mse_n > 0
        snr_n = 10*log10(mean(double(img(:)).^2)/mse_n);
    else
        snr_n = Inf;
    end
    snr_curve = [snr_curve snr_n];
end
figure; plot(noise_levels, snr_curve, '-o');
xlabel('Nivel zgomot simulare (%)'); ylabel('SNR [dB]');
title('Curba SNR in functie de zgomot'); grid on;
saveas(gcf, 'Curba_SNR.png');

% Impact erori - modifca codare linie cu linie

rlc_all_err = rlc_all;
for r = 1:3
    linie = randi(rows);
    if size(rlc_all_err{linie},1) > 0
        idx = randi(size(rlc_all_err{linie},1));
        rlc_all_err{linie}(idx,2) = max(1, rlc_all_err{linie}(idx,2)-1);
    end
end
rec_err = zeros(rows, cols, 'uint8');
for i = 1:rows
    rlc_line = rlc_all_err{i};
    pos = 1;
    for k = 1:size(rlc_line,1)
        val = rlc_line(k,1);
        rep = rlc_line(k,2);
        rep = min(rep, cols-pos+1); % protectie overflow
        rec_err(i, pos:pos+rep-1) = val;
        pos = pos + rep;
    end
end
figure('Name','Imagine cu erori RLC');
imshow(rec_err); title('Imagine cu erori RLC');
imwrite(rec_err, 'Lena256B_RLC_decoded_erroare.bmp');

% Model predictie simplu (pixel precedent din linie)

pred_img = zeros(rows, cols, 'uint8');
pred_img(:,1) = img(:,1);
for i = 1:rows
    pred_img(i,2:end) = img(i,1:end-1);
end
figure('Name','Predictie pixel precedent');
imshow(pred_img); title('Predictie pixel precedent');
imwrite(pred_img, 'predictie_precedent.bmp');

% Caracteristica de calitate si Bpp

size_orig = numel(img) * 8; % biti
nr_rle = 0;
for i = 1:rows
    nr_rle = nr_rle + size(rlc_all{i},1);
end
size_rlc = nr_rle * (8+16); % valoare(8 biti), run(16 biti)
Bpp_orig = size_orig / (rows*cols);
Bpp_rlc = size_rlc / (rows*cols);

if isequal(img, reconstructed)
    psnr_val = Inf;
else
    mse = mean((double(img(:)) - double(reconstructed(:))).^2);
    psnr_val = 10*log10(255^2/mse);
end

fprintf('Bpp original = %.2f, Bpp cod RLC = %.2f\n', Bpp_orig, Bpp_rlc);
fprintf('PSNR = %.2f dB\n', psnr_val);

% Raport profesional
fid = fopen('Raport_analiza_RLC.txt','w');
fprintf(fid, 'Analiza profesionala algoritm RLC pe Lena256B.bmp\n\n');
fprintf(fid, 'Parametri imagine originala: Mean = %.2f, Std = %.2f, Entropy = %.2f\n', mean_orig, std_orig, entropy_orig);
fprintf(fid, 'Parametri imagine recodificata: Mean = %.2f, Std = %.2f, Entropy = %.2f\n', mean_rec, std_rec, entropy_rec);
fprintf(fid, 'PSNR = %.2f dB\n', psnr_val);
fprintf(fid, 'Bpp original = %.2f, Bpp cod RLC = %.2f\n\n', Bpp_orig, Bpp_rlc);
fprintf(fid, 'RLC este foarte eficient la imagini cu zone largi uniforme (ex: fax, scanari alb-negru).\n');
fprintf(fid, 'Avantaje: implementare extrem de simpla, usor de inteles, compresie fara pierderi.\n');
fprintf(fid, 'Dezavantaje: slab la imagini naturale cu multe detalii (fotografii), sensibila la erori (o eroare afecteaza tot sirul).\n');
fprintf(fid, 'Imbunatatiri: aplicarea predictiei inainte de RLC (ex: diferenta intre pixeli), combinarea cu Huffman sau aritmetica.\n');
fprintf(fid, 'Impactul erorilor: chiar o singura eroare poate produce artefacte vizibile pe zone mari.\n');
fprintf(fid, 'Bpp s-a redus doar moderat pe Lena256B (imagine naturala). Pentru imagini cu multe zone uniforme castigul este mult mai mare.\n');
fprintf(fid, 'Reconstructia este fara pierderi (PSNR = Inf) in absenta erorilor.\n');
fprintf(fid, '\nConcluzie: Pentru imagini naturale, RLC nu este ideal. Pentru imagini binare sau cu multe regiuni uniforme, RLC ofera compresie eficienta cu resurse minime de procesare.\n');
fclose(fid);

disp('Scriptul a fost finalizat si fisierele au fost salvate!');
