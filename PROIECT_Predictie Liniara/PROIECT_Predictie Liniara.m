% Tema 2 - Predictie Liniara © by Popescu Vlad Gabriel @ M1_PCON - Matlab v.R2021b

clear all

% Capitolul 2: Deschiderea și Afișarea Semnalului Înregistrat

% Deschidere fișier audio

[file, Fs] = audioread('Anticonstituționalitate.wav');

%[file, Fs] = audioread('/Users/Je/Desktop/Lab_PCSV_V.Popa/L2/Popescu_Vlad_L2/Anticonstitutionalitate.wav');

% Vector timp
t = (0:length(file)-1) / Fs;

% Grafic semnal
figure;
plot(t, file);
title('Semnalul în funcție de timp');
xlabel('Timp [s]');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul2_afisare_semnal.png');

% Capitolul 3: Identificarea Secțiunii Vocale și Determinarea Mediei

% Selectarea unei secțiuni vocale

start_sample = round(Fs * 1); % Exemplu: start la 1s
end_sample = start_sample + 220 - 1;
cadru = file(start_sample:end_sample);

% Calculul mediei
m = mean(cadru);

% Grafic cadru
figure;
plot(cadru);
title('Cadru selectat din semnalul vocal');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul3_cadru_selectat.png');

% Capitolul 4: Determinarea Coeficienților de Predicție Antegradă și Retrogradă

% Calcul autocorelație pentru semnal
M = 14;
R = xcorr(cadru, M, 'biased');

% Matrice autocorelație
R_matrix = toeplitz(R(M+1:2*M));
r_vector = R(M+2:2*M+1);

% Coeficienți predicție antegradă
a = R_matrix \ r_vector;

% Coeficienți predicție retrogradă
b = flipud(a);

% Afișare coeficienți
disp('Coeficienții predicție antegradă:');
disp(a');

disp('Coeficienții predicție retrogradă:');
disp(b');

% Salvare coeficienți
save('capitolul4_coeficienti.mat', 'a', 'b');

% Grafic coeficienți
figure;
subplot(2, 1, 1);
stem(a, 'filled');
title('Coeficienții predicție antegradă');
xlabel('Indice');
ylabel('Valoare');

grid on;
subplot(2, 1, 2);
stem(b, 'filled');
title('Coeficienții predicție retrogradă');
xlabel('Indice');
ylabel('Valoare');
grid on;
saveas(gcf, 'capitolul4_coeficienti.png');

% Capitolul 5: Calculul Erorilor de Predicție

% Erori de predicție
fM = filter([1; -a], 1, cadru);
gM = filter([1; -b], 1, cadru);

% Grafic erori
figure;
subplot(2, 1, 1);
plot(fM);
title('Eroare predicție antegradă');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;

subplot(2, 1, 2);
plot(gM);
title('Eroare predicție retrogradă');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul5_erori_predictie.png');

% Capitolul 6: Reprezentarea Semnalului și Erorilor de Predicție

% Grafic semnal și erori utilizând subplot
figure;
subplot(3, 1, 1);
plot(cadru);
title('Semnalul inițial');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 2);
plot(fM);
title('Eroare predicție antegradă');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 3);
plot(gM);
title('Eroare predicție retrogradă');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul6_semnal_si_erori.png');

% Capitolul 7: Reprezentarea Spectrului și Analiza Comparativă

% Reprezentarea spectrelor fără hold on
figure;
subplot(3, 1, 1);
plot(abs(fft(cadru)));
title('Spectru semnal inițial');
xlabel('Frecvență [Hz]');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 2);
plot(abs(fft(fM)));
title('Spectru eroare antegradă');
xlabel('Frecvență [Hz]');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 3);
plot(abs(fft([1; -a])));
title('Spectru funcție de transfer antegradă');
xlabel('Frecvență [Hz]');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul7_spectru.png');

% Capitolul 8: Analiza Suplimentară a Fonemelor

% 8.1: Identificarea și Selectarea Unei Noi Secțiuni Vocale


% Selectarea unei noi secțiuni vocale
start_sample_2 = round(Fs * 2); % Exemplu: start la 2s
end_sample_2 = start_sample_2 + 220 - 1;
cadru_2 = file(start_sample_2:end_sample_2);

% Calculul mediei
m2 = mean(cadru_2);

% Grafic cadru
figure;
plot(cadru_2);
title('Cadru selectat din semnalul vocal (8.1)');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_1_cadru_selectat.png');


% 8.2: Identificarea și Selectarea Unei Alte Secțiuni Vocale


% Selectarea unei alte secțiuni vocale
start_sample_3 = round(Fs * 3); % Exemplu: start la 3s
end_sample_3 = start_sample_3 + 220 - 1;
cadru_3 = file(start_sample_3:end_sample_3);

% Calculul mediei
m3 = mean(cadru_3);

% Grafic cadru
figure;
plot(cadru_3);
title('Cadru selectat din semnalul vocal (8.2)');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_2_cadru_selectat.png');


% 8.3: Identificarea și Selectarea Unei Alte Secțiuni Vocale

% Selectarea unei alte secțiuni vocale
start_sample_4 = round(Fs * 4); % Exemplu: start la 4s
end_sample_4 = start_sample_4 + 220 - 1;
cadru_4 = file(start_sample_4:end_sample_4);

% Calculul mediei
m4 = mean(cadru_4);

% Grafic cadru
figure;
plot(cadru_4);
title('Cadru selectat din semnalul vocal (8.3)');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_3_cadru_selectat.png');


% 8.4: Determinarea Coeficienților de Predicție Antegradă și Retrogradă

% Calcul autocorelație pentru cadru_2
M = 14;
R_2 = xcorr(cadru_2, M, 'biased');
R_matrix_2 = toeplitz(R_2(M+1:2*M));
r_vector_2 = R_2(M+2:2*M+1);
a_2 = R_matrix_2 \ r_vector_2;
b_2 = flipud(a_2);

% Salvare coeficienți cadru_2
save('capitolul8_4_coeficienti_cadru2.mat', 'a_2', 'b_2');

% Calcul autocorelație pentru cadru_3
R_3 = xcorr(cadru_3, M, 'biased');
R_matrix_3 = toeplitz(R_3(M+1:2*M));
r_vector_3 = R_3(M+2:2*M+1);
a_3 = R_matrix_3 \ r_vector_3;
b_3 = flipud(a_3);

% Salvare coeficienți cadru_3
save('capitolul8_4_coeficienti_cadru3.mat', 'a_3', 'b_3');

% Calcul autocorelație pentru cadru_4
R_4 = xcorr(cadru_4, M, 'biased');
R_matrix_4 = toeplitz(R_4(M+1:2*M));
r_vector_4 = R_4(M+2:2*M+1);
a_4 = R_matrix_4 \ r_vector_4;
b_4 = flipud(a_4);

% Salvare coeficienți cadru_4
save('capitolul8_4_coeficienti_cadru4.mat', 'a_4', 'b_4');

% 8.5: Determinarea Erorilor de Predicție


% Erori de predicție pentru cadru_2
fM_2 = filter([1; -a_2], 1, cadru_2);
gM_2 = filter([1; -b_2], 1, cadru_2);
save('capitolul8_5_erori_cadru2.mat', 'fM_2', 'gM_2');

% Erori de predicție pentru cadru_3
fM_3 = filter([1; -a_3], 1, cadru_3);
gM_3 = filter([1; -b_3], 1, cadru_3);
save('capitolul8_5_erori_cadru3.mat', 'fM_3', 'gM_3');

% Erori de predicție pentru cadru_4
fM_4 = filter([1; -a_4], 1, cadru_4);
gM_4 = filter([1; -b_4], 1, cadru_4);
save('capitolul8_5_erori_cadru4.mat', 'fM_4', 'gM_4');

% 8.6: Reprezentarea Grafică a Semnalului și Erorilor de Predicție


% Reprezentarea grafică pentru cadru_2
figure;
subplot(3, 1, 1);
plot(cadru_2);
title('Semnal inițial (cadru 2)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 2);
plot(fM_2);
title('Eroare de predicție antegradă (cadru 2)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 3);
plot(gM_2);
title('Eroare de predicție retrogradă (cadru 2)');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_6_grafic_cadru2.png');

% Reprezentarea grafică pentru cadru_3
figure;
subplot(3, 1, 1);
plot(cadru_3);
title('Semnal inițial (cadru 3)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 2);
plot(fM_3);
title('Eroare de predicție antegradă (cadru 3)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 3);
plot(gM_3);
title('Eroare de predicție retrogradă (cadru 3)');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_6_grafic_cadru3.png');

% Reprezentarea grafică pentru cadru_4
figure;
subplot(3, 1, 1);
plot(cadru_4);
title('Semnal inițial (cadru 4)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 2);
plot(fM_4);
title('Eroare de predicție antegradă (cadru 4)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 3);
plot(gM_4);
title('Eroare de predicție retrogradă (cadru 4)');
xlabel('Eșantion');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_6_grafic_cadru4.png');


% 8.7: Reprezentarea Grafică a Spectrului Semnalului și Erorilor


% Analiza spectrală pentru cadru_2
fft_cadru_2 = abs(fft(cadru_2));
fft_fM_2 = abs(fft(fM_2));
fft_transfer_2 = abs(fft([1; -a_2]));

figure;
subplot(3, 1, 1);
plot(fft_cadru_2);
title('Spectru semnal inițial (cadru 2)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 2);
plot(fft_fM_2);
title('Spectru eroare antegradă (cadru 2)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 3);
plot(fft_transfer_2);
title('Spectru funcție transfer (cadru 2)');
xlabel('Frecvență [Hz]');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_7_spectru_cadru2.png');


% Analiza spectrală pentru cadru_3
fft_cadru_3 = abs(fft(cadru_3));
fft_fM_3 = abs(fft(fM_3));
fft_transfer_3 = abs(fft([1; -a_3]));

figure;
subplot(3, 1, 1);
plot(fft_cadru_3);
title('Spectru semnal inițial (cadru 3)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 2);
plot(fft_fM_3);
title('Spectru eroare antegradă (cadru 3)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 3);
plot(fft_transfer_3);
title('Spectru funcție transfer (cadru 3)');
xlabel('Frecvență [Hz]');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_7_cadru3.png');

% Analiza spectrală pentru cadru_4
fft_cadru_4 = abs(fft(cadru_4));
fft_fM_4 = abs(fft(fM_4));
fft_transfer_4 = abs(fft([1; -a_4]));

figure;
subplot(3, 1, 1);
plot(fft_cadru_4);
title('Spectru semnal inițial (cadru 4)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 2);
plot(fft_fM_4);
title('Spectru eroare antegradă (cadru 4)');
ylabel('Amplitudine');
grid on;

subplot(3, 1, 3);
plot(fft_transfer_4);
title('Spectru funcție transfer (cadru 4)');
xlabel('Frecvență [Hz]');
ylabel('Amplitudine');
grid on;
saveas(gcf, 'capitolul8_7_cadru4.png');





