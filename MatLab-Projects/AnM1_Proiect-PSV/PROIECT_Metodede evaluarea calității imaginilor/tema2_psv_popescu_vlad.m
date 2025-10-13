clear all

% Tema 2 PSV ©created by Popescu Vlad Gabriel_M1_PCON

% Citirea imaginilor originale și distorsionate

a1 = imread('lighthouse1.jpg'); % imaginea distorsionată 1
a2 = imread('lighthouse2.jpg'); % imaginea distorsionată 2
ref = imread('lighthouse.bmp'); % imaginea de referință

% Calculul MSE și PSNR pentru imaginile distorsionate a1 și a2

mse_a1 = immse(a1, ref);
mse_a2 = immse(a2, ref);
psnr_a1 = psnr(a1, ref);
psnr_a2 = psnr(a2, ref);

% Afișarea valorilor MSE și PSNR calculate

fprintf('MSE pentru a1: %0.4f, PSNR pentru a1: %0.2f dB\n', mse_a1, psnr_a1);
fprintf('MSE pentru a2: %0.4f, PSNR pentru a2: %0.2f dB\n', mse_a2, psnr_a2);

% Calculul scorului SSIM pentru imaginile distorsionate a1 și a2

ssim_a1 = ssim(a1, ref);
ssim_a2 = ssim(a2, ref);

% Afișarea valorilor SSIM

fprintf('SSIM pentru a1: %0.4f\n', ssim_a1);
fprintf('SSIM pentru a2: %0.4f\n', ssim_a2);

% Afișarea hărților SSIM pentru imaginile a1 și a2

[~, ssim_map_a1] = ssim(a1, ref);
[~, ssim_map_a2] = ssim(a2, ref);

figure;
subplot(1, 2, 1), imshow(ssim_map_a1, []), title('Harta SSIM - a1');
subplot(1, 2, 2), imshow(ssim_map_a2, []), title('Harta SSIM - a2');

% Crearea și evaluarea imaginilor distorsionate cu zgomot

halep_img = imread('Halep.jpg');
hummingbird_img = imread('Hummingbird.jpg');
noise_types = {'poisson', 'salt & pepper', 'speckle'};

for i = 1:length(noise_types)
    noisy_halep = imnoise(halep_img, noise_types{i});
    noisy_hummingbird = imnoise(hummingbird_img, noise_types{i});

    % Calcul SNR și SSIM pentru Halep
    [~, snr_halep] = psnr(noisy_halep, halep_img);
    ssim_halep = ssim(noisy_halep, halep_img);

    % Calcul SNR și SSIM pentru Hummingbird
    [~, snr_hummingbird] = psnr(noisy_hummingbird, hummingbird_img);
    ssim_hummingbird = ssim(noisy_hummingbird, hummingbird_img);

    fprintf('SNR și SSIM pentru zgomot %s - Halep: %0.2f dB, %0.4f\n', noise_types{i}, snr_halep, ssim_halep);
    fprintf('SNR și SSIM pentru zgomot %s - Hummingbird: %0.2f dB, %0.4f\n', noise_types{i}, snr_hummingbird, ssim_hummingbird);
end

% Evaluarea compresiei JPEG pentru imaginile "Hummingbird" și "Halep"

qualityFactor = 10:10:100;
ssimValues_halep = zeros(1,10);
snrValues_halep = zeros(1,10);
ssimValues_hummingbird = zeros(1,10);
snrValues_hummingbird = zeros(1,10);

for i = 1:10
    imwrite(halep_img, 'compressedHalep.jpg', 'jpg', 'quality', qualityFactor(i));
    compressed_halep = imread('compressedHalep.jpg');
    [~, snrValues_halep(i)] = psnr(compressed_halep, halep_img);
    ssimValues_halep(i) = ssim(compressed_halep, halep_img);

    imwrite(hummingbird_img, 'compressedHummingbird.jpg', 'jpg', 'quality', qualityFactor(i));
    compressed_hummingbird = imread('compressedHummingbird.jpg');
    [~, snrValues_hummingbird(i)] = psnr(compressed_hummingbird, hummingbird_img);
    ssimValues_hummingbird(i) = ssim(compressed_hummingbird, hummingbird_img);
end

% Afișarea graficelor pentru calitatea imaginii în funcție de factorul de compresie JPEG

figure;
subplot(2, 2, 1), plot(qualityFactor, ssimValues_halep, 'b-o'), title('SSIM vs Quality - Halep');
subplot(2, 2, 2), plot(qualityFactor, snrValues_halep, 'r-*'), title('SNR vs Quality - Halep');
subplot(2, 2, 3), plot(qualityFactor, ssimValues_hummingbird, 'b-o'), title('SSIM vs Quality - Hummingbird');
subplot(2, 2, 4), plot(qualityFactor, snrValues_hummingbird, 'r-*'), title('SNR vs Quality - Hummingbird');
