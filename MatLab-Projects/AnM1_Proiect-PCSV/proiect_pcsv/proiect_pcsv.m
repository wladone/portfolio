clear all

% Tema 1 PCSV Created by Popescu Vlad Gabriel_M1_PCON

% Punctul 1: Încărcarea fișierelor audio și afișarea în funcție de timp

[female_audio, fs] = audioread('casa_female.wav');
[male1_audio, ~] = audioread('casa_male1.wav');
[male2_audio, ~] = audioread('casa_male2.wav');

t_female = (0:length(female_audio)-1) / fs;
t_male1 = (0:length(male1_audio)-1) / fs;
t_male2 = (0:length(male2_audio)-1) / fs;

% Vizualizare semnale audio în funcție de timp

figure;
subplot(3,1,1); plot(t_female, female_audio); title('Semnal Feminin'); xlabel('Timp (s)'); ylabel('Amplitudine');
subplot(3,1,2); plot(t_male1, male1_audio); title('Semnal Masculin 1'); xlabel('Timp (s)'); ylabel('Amplitudine');
subplot(3,1,3); plot(t_male2, male2_audio); title('Semnal Masculin 2'); xlabel('Timp (s)'); ylabel('Amplitudine');

% Punctul 2: Calcularea Puterii pe Termen Scurt

frame_size = round(0.03 * fs); % Lungimea ferestrei de 30 ms
overlap = round(0.015 * fs);   % Suprapunere de 15 ms

% Calculul puterii pe termen scurt.
% Folosim `buffer`care împarte fiecare semnal în ferestre cu suprapunere 
% și calculează puterea fiecărei ferestre și `mean` pentru a obține puterea medie în ferestre.

female_power = buffer(female_audio.^2, frame_size, overlap, 'nodelay');
female_power = mean(female_power, 1);

male1_power = buffer(male1_audio.^2, frame_size, overlap, 'nodelay');
male1_power = mean(male1_power, 1);

male2_power = buffer(male2_audio.^2, frame_size, overlap, 'nodelay');
male2_power = mean(male2_power, 1);

% Punctul 3: Calcularea Rata Trecerilor prin Zero.Determinăm rata trecerilor prin zero 

% folosind o funcție anonimă: calculate_zc_rate care calculează trecerile prin zero 
% pentru fiecare fereastră și `buffer` pentru a fragmenta semnalul.

calculate_zc_rate = @(x) sum(diff(sign(x)) ~= 0) / length(x);
female_zc = buffer(female_audio, frame_size, overlap, 'nodelay');
female_zc_avg = mean(arrayfun(calculate_zc_rate, female_zc));

male1_zc = buffer(male1_audio, frame_size, overlap, 'nodelay');
male1_zc_avg = mean(arrayfun(calculate_zc_rate, male1_zc));

male2_zc = buffer(male2_audio, frame_size, overlap, 'nodelay');
male2_zc_avg = mean(arrayfun(calculate_zc_rate, male2_zc));

% Punctul 4: Calcularea Autocorelatiei pe Termen Scurt.

% Aplicăm `xcorr` pentru autocorelația fiecărei ferestre,  în funcție de semnal.

calculate_acf = @(x) xcorr(x, 'biased');
female_acf = cell2mat(arrayfun(@(i) calculate_acf(female_audio(i:i+frame_size-1)), 1:overlap:length(female_audio)-frame_size, 'UniformOutput', false));
male1_acf = cell2mat(arrayfun(@(i) calculate_acf(male1_audio(i:i+frame_size-1)), 1:overlap:length(male1_audio)-frame_size, 'UniformOutput', false));
male2_acf = cell2mat(arrayfun(@(i) calculate_acf(male2_audio(i:i+frame_size-1)), 1:overlap:length(male2_audio)-frame_size, 'UniformOutput', false));

% Punctul 5: Calcularea Densității Spectrale de Putere.

% Folosim FFT pentru calculul densității spectrale, urmată de modulul pătrat, pentru psd.

calculate_psd = @(x) abs(fft(x)).^2;
female_psd = cell2mat(arrayfun(@(i) calculate_psd(female_audio(i:i+frame_size-1)), 1:overlap:length(female_audio)-frame_size, 'UniformOutput', false));
male1_psd = cell2mat(arrayfun(@(i) calculate_psd(male1_audio(i:i+frame_size-1)), 1:overlap:length(male1_audio)-frame_size, 'UniformOutput', false));
male2_psd = cell2mat(arrayfun(@(i) calculate_psd(male2_audio(i:i+frame_size-1)), 1:overlap:length(male2_audio)-frame_size, 'UniformOutput', false));

% Punctul 6: Calcularea Diferenței Medii de Amplitudine.
% difference_amp calculează media diferențelor de amplitudine între eșantioane consecutive în fiecare fereastră.

difference_amp = @(x) mean(abs(diff(x)));
female_diff = arrayfun(@(i) difference_amp(female_audio(i:i+frame_size-1)), 1:overlap:length(female_audio)-frame_size);
male1_diff = arrayfun(@(i) difference_amp(male1_audio(i:i+frame_size-1)), 1:overlap:length(male1_audio)-frame_size);
male2_diff = arrayfun(@(i) difference_amp(male2_audio(i:i+frame_size-1)), 1:overlap:length(male2_audio)-frame_size);

% Punctul 7: Vizualizarea și Compararea Rezultatelor.
% Folosim subplot pentru a aranja rezultatele comparative în ferestre separate, facilitând analiza vizuală a rezultatelor.

% 1. Puterea pe Termen Scurt

figure;
subplot(2,3,1);
plot(female_power, 'r'); hold on; plot(male1_power, 'g'); plot(male2_power, 'b');
title('Putere pe Termen Scurt'); xlabel('Timp (ferestre)'); ylabel('Putere Medie');
legend('Femeie', 'Barbat 1', 'Barbat 2');

% 2. Rata Trecerilor prin Zero

subplot(2,3,2);
bar([female_zc_avg, male1_zc_avg, male2_zc_avg]);
set(gca, 'XTickLabel', {'Femeie', 'Barbat 1', 'Barbat 2'}); title('Rata Trecerilor prin Zero'); ylabel('Rata medie');

% 3. Autocorelatie pe Termen Scurt

subplot(2,3,3);
plot(female_acf, 'r'); hold on; plot(male1_acf, 'g'); plot(male2_acf, 'b');
title('Autocorelatie pe Termen Scurt'); xlabel('Lag'); ylabel('Autocorelație');

% 4. Densitate Spectrala de Putere

subplot(2,3,4);
plot(female_psd, 'r'); hold on; plot(male1_psd, 'g'); plot(male2_psd, 'b');
title('Densitate Spectrala de Putere'); xlabel('Frecvență'); ylabel('PSD');

% 5. Diferența Medie de Amplitudine

subplot(2,3,5);
plot(female_diff, 'r'); hold on; plot(male1_diff, 'g'); plot(male2_diff, 'b');
title('Diferența de Amplitudine'); xlabel('Timp (ferestre)'); ylabel('Diferența Medie');

% Salvarea rezultatelor în fișiere pentru referință ulterioară

save('power_results.mat', 'female_power', 'male1_power', 'male2_power');
save('zero_crossing_results.mat', 'female_zc_avg', 'male1_zc_avg', 'male2_zc_avg');
save('autocorrelation_results.mat', 'female_acf', 'male1_acf', 'male2_acf');
save('psd_results.mat', 'female_psd', 'male1_psd', 'male2_psd');
save('amplitude_diff_results.mat', 'female_diff', 'male1_diff', 'male2_diff');
