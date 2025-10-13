% Tema 4 - Semnale şi Sisteme Bidimensionale - MATLAB Script © created by Popescu Vlad Gabriel M1/PCON

clear all;

% Aplicația 1: Generarea imaginilor folosind funcții bidimensionale

% 1.1 Reprezentarea impulsului Dirac bidimensional
im = zeros(128);
im(64,64) = 1;
figure(1), colormap(gray), title('Functia Dirac Bidimensional');
imagesc(im);
saveas(gcf, 'impuls_dirac.png');

% 1.2 Reprezentarea impulsului dreptunghiular bidimensional
im = zeros(128);
im(20:40, 20:40) = 1;
figure(2), colormap(gray), title('Impuls dreptunghiular');
imagesc(im);
saveas(gcf, 'impuls_dreptunghiular.png');

% 1.3 Reprezentarea funcțiilor sinus și cosinus bidimensionale
m = 128; n = 128;
tm = (0:1:(m-1))/m;
tn = (0:1:(n-1))/n;
k = 3; l = -5;
i = sqrt(-1);
wave = exp(2*pi*i*k*tm') * exp(2*pi*i*l*tn);
figure(3), colormap(gray), imagesc(real(wave)), title('Functia Cosinus Bidimensional');
saveas(gcf, 'cos_bidimensional.png');
figure(4), colormap(gray), imagesc(imag(wave)), title('Functia Sinus Bidimensional');
saveas(gcf, 'sin_bidimensional.png');

% 1.4 Funcție Matlab pentru generarea unei matrici pătratice
A = (1:3)' * ones(1,3);
figure(5), imagesc(A), title('Matricea A');
saveas(gcf, 'matrice_a.png');

% Aplicația 2: Structuri periodice

% 2.1 Structuri periodizate
% Generarea structurii periodice corespunzătoare unei perioade
A = ones(20, 60);
A(:, 21:40) = 0;
figure(6), imagesc(A), colormap(gray), title('Structura Periodică - O Perioadă');
saveas(gcf, 'structura_perioada.png');

% Reprezentarea structurii periodizate pe 6 perioade
A_periodic = repmat(A, [1, 6]);
figure(7), imagesc(A_periodic), colormap(gray), title('Structura Periodizată pe 6 Perioade');
saveas(gcf, 'structura_periodizata_6.png');

% Generarea și salvarea tuturor structurilor din Tabelul 4.2
B = ones(20, 60);
B(11:20, :) = 0;
figure(8), imagesc(B), colormap(gray), title('Structura B din Tabelul 4.2');
saveas(gcf, 'structura_B.png');

C = repmat([ones(10, 20), zeros(10, 20)], [1, 3]);
figure(9), imagesc(C), colormap(gray), title('Structura C din Tabelul 4.2');
saveas(gcf, 'structura_C.png');

% Aplicația 3: Imagini binare (halftoning)

% 3.1 Convertirea imaginii "lena.bmp" la o imagine binară
A = imread('lena.bmp');
BW = im2bw(A, 0.5);
figure(10), imshow(BW), title('Imagine Binară lena2');
saveas(gcf, 'lena2.bmp');

% 3.2 Obținerea imaginii eroare de cuantizare
A = imread('lena.bmp');
BW = im2bw(A, 0.5);
err_image = abs(double(A) - double(BW) * 255);
figure(11), imshow(uint8(err_image)), title('Imagine Eroare de Cuantizare');
saveas(gcf, 'err1.bmp');

% 3.3 Binarizarea folosind pragul Otsu
level = graythresh(A);
BW_otsu = imbinarize(A, level);
figure(12), imshow(BW_otsu), title('Imagine Binară cu Prag Otsu');
saveas(gcf, 'lena_otsu.bmp');

% 3.4 Binarizarea adaptivă
BW_adaptive = imbinarize(A, 'adaptive', 'Sensitivity', 0.5);
figure(13), imshow(BW_adaptive), title('Imagine Binară Adaptivă');
saveas(gcf, 'lena_adaptive.bmp');

% Aplicația 4: Pseudocolorare cu palete de culori

% 4.1 Pseudocolorarea imaginii "spine.tif"
A = imread('spine.tif');
X = grayslice(A, 16);
figure(14), imshow(X, hot(16)), title('Pseudocolorare - Paleta Hot');
saveas(gcf, 'spine_hot.png');

% 4.2 Afișarea cu diverse palete de culori
figure(15), imshow(X, jet(16)), title('Pseudocolorare - Paleta Jet');
saveas(gcf, 'spine_jet.png');
figure(16), imshow(X, pink(16)), title('Pseudocolorare - Paleta Pink');
saveas(gcf, 'spine_pink.png');
figure(17), imshow(X, hsv(16)), title('Pseudocolorare - Paleta HSV');
saveas(gcf, 'spine_hsv.png');

% Aplicația 5: Colorarea fundalului imaginii "test.tif"

% 5.1 Colorarea fundalului cu verde
BW = roicolor(A, 0, 50);
A_colored = A;
A_colored(BW) = 0;
figure(18), imshow(A_colored), title('Fundal Colorat cu Verde');
saveas(gcf, 'test_colored_green.png');

% Aplicația 6: Segmentarea manuală a imaginii "pasare.jpg"

% 6.1 Segmentarea manuală cu roipoly
A = imread('pasare.jpg');
BW = roipoly(A);
figure(19), imshow(BW), title('Segmentare Manuală - Pasare');
saveas(gcf, 'pasare_segmentata.png');

% Îmbunătățirea scriptului pentru segmentare manuală
% După selectarea manuală, scriptul continuă automat
disp('Apăsați Enter sau faceți click stânga pentru a încheia selectarea.');
BW = roipoly(A);
figure(19), imshow(BW), title('Segmentare Manuală - Pasare');
saveas(gcf, 'pasare_segmentata.png');

% 6.2 Colorarea fundalului cu verde
A_segmented = A;
A_segmented(~BW) = 0;
figure(20), imshow(A_segmented), title('Pasare Segmentată cu Fundal Verde');
saveas(gcf, 'pasare_fundal_verde.png');

% Aplicația 7: Segmentarea în spațiul HSV

% 7.1 Transformarea în spațiul HSV
A_hsv = rgb2hsv(A);
H = A_hsv(:,:,1);
figure(21), imshow(H), title('Componenta H a imaginii Pasare');
saveas(gcf, 'pasare_h_component.png');

% 7.2 Selectarea culorilor pentru segmentare
impixelinfo;

% 7.3 Segmentarea folosind roicolor
min_value = 0.1; % Prag minim ales pentru segmentare
max_value = 0.5; % Prag maxim ales pentru segmentare
BW = roicolor(H, min_value, max_value);
figure(22), imshow(BW), title('Mască Segmentare Pasare');
saveas(gcf, 'pasare_mask.png');

% 7.4 Aplicarea măștii și eliminarea fundalului
A_segmented = A;
A_segmented(~BW) = 0;
figure(23), imshow(A_segmented), title('Pasare fără Fundal');
saveas(gcf, 'pasare_fara_fundal.png');
