clear all

% Importarea semnalelor audio convertite în format WAV
[audio1, fs1] = audioread('Auditie_1.wav');
[audio2, fs2] = audioread('Auditie_2.wav');
[audio3, fs3] = audioread('Auditie_3.wav');
[audio4, fs4] = audioread('Auditie_4.wav');
[audio5, fs5] = audioread('Auditie_5.wav');

% Afișarea informațiilor despre fișierele audio
disp(['Durata semnalului 1: ', num2str(length(audio1)/fs1), ' secunde']);
disp(['Durata semnalului 2: ', num2str(length(audio2)/fs2), ' secunde']);
disp(['Durata semnalului 3: ', num2str(length(audio3)/fs3), ' secunde']);
disp(['Durata semnalului 4: ', num2str(length(audio4)/fs4), ' secunde']);
disp(['Durata semnalului 5: ', num2str(length(audio5)/fs5), ' secunde']);

% Calcularea valorii RMS pentru fiecare semnal
rms1 = sqrt(mean(audio1.^2));
rms2 = sqrt(mean(audio2.^2));
rms3 = sqrt(mean(audio3.^2));
rms4 = sqrt(mean(audio4.^2));
rms5 = sqrt(mean(audio5.^2));

% Calculul diferențelor de intensitate
intensityDifferences = [abs(rms1 - rms2), abs(rms2 - rms3), abs(rms3 - rms4), abs(rms4 - rms5)];
disp('Diferențele de intensitate între semnale:');
disp(intensityDifferences);

% Construirea funcției psihometrice

% Date experimentale
stimulusLevels = [1, 2, 3, 4, 5]; % Exemplu de niveluri ale stimulilor
proportions = [0.1, 0.2, 0.5, 0.8, 0.9]; % Exemplu de proporții "≠"

% Funcția logistică
sigmoid = @(p, x) 1 ./ (1 + exp(-(x - p(1)) / p(2)));

% Ajustarea funcției psihometrice
p0 = [median(stimulusLevels), 1]; % Estimări inițiale
p = nlinfit(stimulusLevels, proportions, sigmoid, p0);

% Generarea graficului
x_fit = linspace(min(stimulusLevels), max(stimulusLevels), 100);
y_fit = sigmoid(p, x_fit);

figure;
scatter(stimulusLevels, proportions, 'filled'); % Date experimentale
hold on;
plot(x_fit, y_fit, 'r-', 'LineWidth', 2); % Funcția ajustată
xlabel('Nivelul stimulului (\Delta I)');
ylabel('Proporția răspunsurilor "≠"');
title('Funcția psihometrică ajustată');
grid on;






