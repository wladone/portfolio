%  - Determinarea experimentala (folosind metoda stimulilor constanti) a pragului diferential pentru intensitate în MATLAB by Vlad Popescu Gabriel

clear all

% Analiza semnalelor audio și determinarea funcției psihometrice

%% 1. Importul fișierelor audio
% Conversia fișierelor .aup în .wav trebuie realizată anterior în Audacity
[audio1, fs1] = audioread('Auditie_1.wav');
[audio2, fs2] = audioread('Auditie_2.wav');
[audio3, fs3] = audioread('Auditie_3.wav');
[audio4, fs4] = audioread('Auditie_4.wav');
[audio5, fs5] = audioread('Auditie_5.wav');

% Afișarea informațiilor despre fișiere
disp('Informații despre fișierele audio:');
disp(['Durata semnalului 1: ', num2str(length(audio1)/fs1), ' secunde']);
disp(['Durata semnalului 2: ', num2str(length(audio2)/fs2), ' secunde']);
disp(['Durata semnalului 3: ', num2str(length(audio3)/fs3), ' secunde']);
disp(['Durata semnalului 4: ', num2str(length(audio4)/fs4), ' secunde']);
disp(['Durata semnalului 5: ', num2str(length(audio5)/fs5), ' secunde']);

%% 2. Calcularea caracteristicilor semnalelor
% Calcularea valorilor RMS pentru fiecare semnal
rms1 = sqrt(mean(audio1.^2));
rms2 = sqrt(mean(audio2.^2));
rms3 = sqrt(mean(audio3.^2));
rms4 = sqrt(mean(audio4.^2));
rms5 = sqrt(mean(audio5.^2));

% Diferențele de intensitate între semnale consecutive
intensityDifferences = [abs(rms1 - rms2), abs(rms2 - rms3), abs(rms3 - rms4), abs(rms4 - rms5)];
disp('Diferențele de intensitate între semnale:');
disp(intensityDifferences);

% Calcularea energiei fiecărui semnal
energy1 = sum(audio1.^2);
energy2 = sum(audio2.^2);
energy3 = sum(audio3.^2);
energy4 = sum(audio4.^2);
energy5 = sum(audio5.^2);
energies = [energy1, energy2, energy3, energy4, energy5];
disp('Energia semnalelor:');
disp(energies);

%% 3. Construirea funcției psihometrice
% Datele experimentale (nivelurile stimulilor și proporțiile răspunsurilor "≠")
stimulusLevels = [1, 2, 3, 4, 5]; % Exemplu de niveluri
proportions = [0.1, 0.2, 0.5, 0.8, 0.9]; % Exemplu de proporții

% Definirea funcției logistice
sigmoid = @(p, x) 1 ./ (1 + exp(-(x - p(1)) / p(2)));

% Ajustarea funcției psihometrice
p0 = [median(stimulusLevels), 1]; % Estimări inițiale pentru x0 și k
p = nlinfit(stimulusLevels, proportions, sigmoid, p0);

% Extragem punctul de inflexiune și panta ajustată
x0 = p(1); % Pragul diferențial
k = p(2); % Panta curbei
disp(['Pragul diferențial (x0): ', num2str(x0)]);
disp(['Panta curbei (k): ', num2str(k)]);

%% 4. Generarea graficului funcției psihometrice
x_fit = linspace(min(stimulusLevels), max(stimulusLevels), 100); % Intervalul pentru curba ajustată
y_fit = sigmoid(p, x_fit);

figure;
scatter(stimulusLevels, proportions, 'filled'); % Punctele experimentale
hold on;
plot(x_fit, y_fit, 'r-', 'LineWidth', 2); % Curba ajustată
xline(x0, 'g--', 'Punct de inflexiune');
xlabel('Nivelul stimulului (\Delta I)');
ylabel('Proporția răspunsurilor "≠"');
title('Funcția psihometrică ajustată');
legend('Date experimentale', 'Funcția ajustată', 'Punctul de inflexiune');
grid on;

%% 5. Salvarea graficului generat
saveas(gcf, 'Grafic_Functie_Psihometrica.png');

%% 6. Analiza diferențelor dintre semnale
% Compararea frecvențelor dominante
fft1 = fft(audio1);
fft2 = fft(audio2);
fft3 = fft(audio3);
fft4 = fft(audio4);
fft5 = fft(audio5);

frequencies1 = (0:length(fft1)-1) * (fs1 / length(fft1));
frequencies2 = (0:length(fft2)-1) * (fs2 / length(fft2));

[~, idx1] = max(abs(fft1));
[~, idx2] = max(abs(fft2));

dominantFreq1 = frequencies1(idx1);
dominantFreq2 = frequencies2(idx2);

disp(['Frecvența dominantă semnal 1: ', num2str(dominantFreq1), ' Hz']);
disp(['Frecvența dominantă semnal 2: ', num2str(dominantFreq2), ' Hz']);
disp(['Diferența de frecvență dominantă: ', num2str(abs(dominantFreq1 - dominantFreq2)), ' Hz']);
