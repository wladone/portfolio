% Tema 5 Filtrarea imaginilor © Popescu Vlad Gabriel_M1_PCON

% Aplicația 1 - Convoluția liniară
clc; clear; close all;

% Citirea imaginii
img = imread('conv.tif');
img = double(img);

% Definirea funcțiilor pondere
h1 = [1 1 1; 0 0 0; -1 -1 -1]; % Filtru h1
h2 = [0 1 2; -1 0 1; -2 -1 0]; % Filtru h2
h4 = [1 1 1; 1 -8 1; 1 1 1];   % Filtru h4

% Convoluție cu fiecare filtru
g1 = conv2(img, h1, 'same');
g2 = conv2(img, h2, 'same');
g4 = conv2(img, h4, 'same');

% Afișarea imaginilor rezultate
figure; imagesc(g1); colormap gray; title('Convoluția cu Filtrul h1');
imwrite(mat2gray(g1), 'conv_h1.png'); % Salvare imagine

figure; imagesc(g2); colormap gray; title('Convoluția cu Filtrul h2');
imwrite(mat2gray(g2), 'conv_h2.png'); % Salvare imagine

figure; imagesc(g4); colormap gray; title('Convoluția cu Filtrul h4');
imwrite(mat2gray(g4), 'conv_h4.png'); % Salvare imagine

% Citirea imaginii lena.bmp
img = imread('lena.bmp');
img = double(img);

% Definirea filtrelor box de diferite dimensiuni
box3 = fspecial('average', [3 3]);    % Filtru box 3x3
box7 = fspecial('average', [7 7]);    % Filtru box 7x7
box11 = fspecial('average', [11 11]); % Filtru box 11x11

% Aplicarea filtrării folosind imfilter
filtered3 = imfilter(img, box3, 'replicate');
filtered7 = imfilter(img, box7, 'replicate');
filtered11 = imfilter(img, box11, 'replicate');

% Afișarea și salvarea imaginilor
figure; imshow(uint8(img)); title('Imaginea Originală - lena.bmp');
imwrite(uint8(img), 'lena_original.png');

figure; imshow(uint8(filtered3)); title('Filtru Box 3x3');
imwrite(uint8(filtered3), 'lena_box3.png');

figure; imshow(uint8(filtered7)); title('Filtru Box 7x7');
imwrite(uint8(filtered7), 'lena_box7.png');

figure; imshow(uint8(filtered11)); title('Filtru Box 11x11');
imwrite(uint8(filtered11), 'lena_box11.png');


% Aplicația 2.2 - Filtrarea cu filtre Gaussiene

% Citirea imaginii peisaj_g.tif
img_noisy = imread('peisaj_g.tif');
img_noisy = double(img_noisy);

% Definirea filtrelor gaussiene
h1 = fspecial('gaussian', [3 3], 0.8);
h2 = fspecial('gaussian', [3 3], 0.5);

% Aplicarea filtrelor gaussiene
filtered_gauss1 = imfilter(img_noisy, h1, 'replicate');
filtered_gauss2 = imfilter(img_noisy, h2, 'replicate');

% Filtrarea cu filtru Box 3x3 pentru comparație
box3 = fspecial('average', [3 3]);
filtered_box3 = imfilter(img_noisy, box3, 'replicate');

% Afișarea și salvarea imaginilor
figure; imshow(uint8(img_noisy)); title('Imaginea Originală - peisaj_g.tif');
imwrite(uint8(img_noisy), 'peisaj_original.png');

figure; imshow(uint8(filtered_gauss1)); title('Filtru Gaussian, Sigma = 0.8');
imwrite(uint8(filtered_gauss1), 'peisaj_gauss_0.8.png');

figure; imshow(uint8(filtered_gauss2)); title('Filtru Gaussian, Sigma = 0.5');
imwrite(uint8(filtered_gauss2), 'peisaj_gauss_0.5.png');

figure; imshow(uint8(filtered_box3)); title('Filtru Box 3x3');
imwrite(uint8(filtered_box3), 'peisaj_box3x3.png');


%  Aplicația 3 - Filtrarea imaginii „grad.tif" cu filtre de gradient de ordin 1 și 2, și adăugarea zgomotului Gaussian

% Aplicația 3.1 - Filtrarea imaginii cu filtre de gradient de ordin 1 și 2

% Citirea imaginii grad.tif
img = imread('grad.tif');
img = double(img);

% Definirea filtrelor gradient din Tabelul 5.1 și 5.2
prewitt_x = fspecial('prewitt');    % Gradient Prewitt pe direcția x
prewitt_y = prewitt_x';             % Gradient Prewitt pe direcția y

sobel_x = fspecial('sobel');        % Gradient Sobel pe direcția x
sobel_y = sobel_x';                 % Gradient Sobel pe direcția y

laplacian = [1 -2 1; -2 4 -2; 1 -2 1]; % Filtru Laplacian (gradient ordin 2)

% Aplicarea filtrelor pe imagine
grad_prewitt_x = imfilter(img, prewitt_x, 'replicate');
grad_prewitt_y = imfilter(img, prewitt_y, 'replicate');

grad_sobel_x = imfilter(img, sobel_x, 'replicate');
grad_sobel_y = imfilter(img, sobel_y, 'replicate');

grad_laplacian = imfilter(img, laplacian, 'replicate');

% Afișarea și salvarea imaginilor rezultate
figure; imshow(uint8(img)); title('Imaginea Originală - grad.tif');
imwrite(uint8(img), 'grad_original.png');

figure; imshow(uint8(grad_prewitt_x)); title('Gradient Prewitt - X');
imwrite(uint8(grad_prewitt_x), 'grad_prewitt_x.png');

figure; imshow(uint8(grad_prewitt_y)); title('Gradient Prewitt - Y');
imwrite(uint8(grad_prewitt_y), 'grad_prewitt_y.png');

figure; imshow(uint8(grad_sobel_x)); title('Gradient Sobel - X');
imwrite(uint8(grad_sobel_x), 'grad_sobel_x.png');

figure; imshow(uint8(grad_sobel_y)); title('Gradient Sobel - Y');
imwrite(uint8(grad_sobel_y), 'grad_sobel_y.png');

figure; imshow(uint8(grad_laplacian)); title('Gradient Laplacian');
imwrite(uint8(grad_laplacian), 'grad_laplacian.png');

% 3.1 Filtrarea imaginii lena.bmp cu filtre Box

img_lena = imread('lena.bmp');

box3 = fspecial('average', [3 3]);
box7 = fspecial('average', [7 7]);
box11 = fspecial('average', [11 11]);

filtered3 = imfilter(img_lena, box3, 'replicate');
filtered7 = imfilter(img_lena, box7, 'replicate');
filtered11 = imfilter(img_lena, box11, 'replicate');

figure; imshow(filtered3); title('Filtru Box 3x3');
imwrite(filtered3, 'lena_box3.png');

figure; imshow(filtered7); title('Filtru Box 7x7');
imwrite(filtered7, 'lena_box7.png');

% Aplicația 3.2 - Adăugare zgomot gaussian și filtrare cu Laplacian

% Citirea imaginii grad.tif
img = imread('grad.tif');
img = double(img);

% Adăugarea zgomotului Gaussian
img_noisy = imnoise(uint8(img), 'gaussian', 0, 0.01);

% Filtru Laplacian
laplacian = [1 -2 1; -2 4 -2; 1 -2 1];
filtered_laplacian = imfilter(double(img_noisy), laplacian, 'replicate');

% Afișarea și salvarea imaginilor
figure; imshow(uint8(img_noisy)); title('Imagine cu Zgomot Gaussian');
imwrite(uint8(img_noisy), 'grad_Gaussian_noisy.png');

figure; imshow(uint8(filtered_laplacian)); title('Filtru Laplacian aplicat pe imaginea zgomotată');
imwrite(uint8(filtered_laplacian), 'grad_laplacian_noisy.png');

% 3.2 Filtrarea imaginii peisaj_g.tif cu Filtre Gaussian

img_peisaj = imread('peisaj_g.tif');

h_gauss1 = fspecial('gaussian', [3 3], 0.8);
h_gauss2 = fspecial('gaussian', [3 3], 0.5);

filtered_gauss1 = imfilter(img_peisaj, h_gauss1, 'replicate');
filtered_gauss2 = imfilter(img_peisaj, h_gauss2, 'replicate');

figure; imshow(filtered_gauss1); title('Filtru Gaussian σ=0.8');
imwrite(filtered_gauss1, 'peisaj_gauss_0.8.png');

figure; imshow(filtered_gauss2); title('Filtru Gaussian σ=0.5');
imwrite(filtered_gauss2, 'peisaj_gauss_0.5.png');

% Aplicația 4: Detectarea contururilor folosind algoritmul Canny

% Aplicația 4.1 - Detectarea contururilor cu algoritmul Canny

% Citirea imaginii canny.jpg
img = imread('canny.jpg');
img_gray = rgb2gray(img); % Conversie în imagine gri dacă este RGB

% Detectarea contururilor folosind algoritmul Canny
BW = edge(img_gray, 'canny');

% Afișarea și salvarea rezultatului
figure; imshow(img_gray); title('Imaginea Originală - canny.jpg');
imwrite(img_gray, 'canny_original.png');

figure; imshow(BW); title('Detecția Contururilor - Algoritmul Canny');
imwrite(BW, 'canny_edges.png');


% Aplicația 4.2 și 4.3 - Modificarea pragurilor algoritmului Canny

% Citirea imaginii
img = imread('canny.jpg');
img_gray = rgb2gray(img);

% Detectarea contururilor cu diferite valori ale pragurilor
BW_low = edge(img_gray, 'canny', [0.05 0.1]);
BW_mid = edge(img_gray, 'canny', [0.1 0.2]);
BW_high = edge(img_gray, 'canny', [0.2 0.4]);

% Afișarea și salvarea rezultatelor
figure; imshow(BW_low); title('Canny - Praguri [0.05, 0.1]');
imwrite(BW_low, 'canny_low_threshold.png');

figure; imshow(BW_mid); title('Canny - Praguri [0.1, 0.2]');
imwrite(BW_mid, 'canny_mid_threshold.png');

figure; imshow(BW_high); title('Canny - Praguri [0.2, 0.4]');
imwrite(BW_high, 'canny_high_threshold.png');


% Aplicația 4.4 - Modificarea valorii SIGMA pentru algoritmul Canny

% Citirea imaginii
img = imread('canny.jpg');
img_gray = rgb2gray(img);

% Detectarea contururilor cu diferite valori SIGMA
BW_sigma_low = edge(img_gray, 'canny', [], 0.5);
BW_sigma_default = edge(img_gray, 'canny', [], sqrt(2));
BW_sigma_high = edge(img_gray, 'canny', [], 3);

% Afișarea și salvarea rezultatelor
figure; imshow(BW_sigma_low); title('Canny - SIGMA = 0.5');
imwrite(BW_sigma_low, 'canny_sigma_0.5.png');

figure; imshow(BW_sigma_default); title('Canny - SIGMA = sqrt(2) (Implicit)');
imwrite(BW_sigma_default, 'canny_sigma_default.png');

figure; imshow(BW_sigma_high); title('Canny - SIGMA = 3');
imwrite(BW_sigma_high, 'canny_sigma_3.png');



% Aplicația 5: Accentuarea contururilor imaginii „zid.tif”

% Aplicația 5.1 - Accentuarea contururilor cu filtru trece-sus (Laplacian)

% Citirea imaginii zid.tif
img = imread('zid.tif');
img = double(img);

% Definirea filtrului Laplacian
laplacian = [1 -2 1; -2 4 -2; 1 -2 1];

% Filtrarea imaginii
filtered_laplacian = imfilter(img, laplacian, 'replicate');

% Însumarea imaginii originale cu versiunea trece-sus
k = 0.5; % Ponderea ajustabilă
sharpened_img = img + k * filtered_laplacian;

% Afișarea și salvarea imaginilor rezultate
figure; imshow(uint8(img)); title('Imaginea Originală - zid.tif');
imwrite(uint8(img), 'zid_original.png');

figure; imshow(uint8(filtered_laplacian)); title('Imagine Filtrată - Laplacian');
imwrite(uint8(filtered_laplacian), 'zid_laplacian.png');

figure; imshow(uint8(sharpened_img)); title(['Imagine Accentuare Contururi, k = ', num2str(k)]);
imwrite(uint8(sharpened_img), 'zid_sharpened.png');


% Aplicația 5.2 - Accentuarea contururilor folosind Unsharp Masking

% Citirea imaginii zid.tif
img = imread('zid.tif');

% Aplicarea unsharp masking folosind funcția imsharpen
sharpened_img_08 = imsharpen(img, 'Amount', 0.8);
sharpened_img_15 = imsharpen(img, 'Amount', 1.5);

% Afișarea și salvarea imaginilor rezultate
figure; imshow(img); title('Imaginea Originală - zid.tif');
imwrite(img, 'zid_original.png');

figure; imshow(sharpened_img_08); title('Unsharp Masking - Amount = 0.8 (Implicit)');
imwrite(sharpened_img_08, 'zid_unsharp_0.8.png');

figure; imshow(sharpened_img_15); title('Unsharp Masking - Amount = 1.5');
imwrite(sharpened_img_15, 'zid_unsharp_1.5.png');


% Aplicația 5.3 - Funcția pondere pentru Unsharp Masking cu filtru Box

% Citirea imaginii zid.tif
img = imread('zid.tif');
img = double(img);

% Definirea filtrului Box 3x3
box3 = fspecial('average', [3 3]);

% Aplicarea filtrului Box (trece-jos)
blurred = imfilter(img, box3, 'replicate');

% Calcularea imaginii unsharp masking
k = 1; % Parametru
unsharp_masking = img + k * (img - blurred);

% Afișarea și salvarea imaginilor rezultate
figure; imshow(uint8(blurred)); title('Imagine Filtrată - Box 3x3');
imwrite(uint8(blurred), 'zid_blurred_box3.png');

figure; imshow(uint8(unsharp_masking)); title(['Unsharp Masking - k = ', num2str(k)]);
imwrite(uint8(unsharp_masking), 'zid_unsharp_box3.png');












