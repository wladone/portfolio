% Tema 3 - Codarea formei © Popescu Vlad Gabriel_M1_PCON

clear all

% 1. Incarcă și afișează fișierul
[file, fs] = audioread('ambient_electronic.wav');
disp(['Rate eșantionare: ', num2str(fs), ' Hz']);
duration = length(file) / fs;
disp(['Durata semnalului: ', num2str(duration), ' secunde']);
disp(['Număr de canale: ', num2str(size(file, 2))]);
sound(file, fs);
figure;
plot(file);
title('Semnal Original'); xlabel('Timp'); ylabel('Amplitudine');

% 2. Reeșantionare
new_fs = 8000;
file_resampled = resample(file, new_fs, fs);
file_resampled = max(min(file_resampled, 1), -1); % Clipping pentru evitarea erorilor
audiowrite('resampled_signal.wav', file_resampled, new_fs);
figure;
plot(file_resampled);
title('Semnal Reeșantionat la 8000 Hz'); xlabel('Timp'); ylabel('Amplitudine');

% 3. Codare PCM pe 2, 4, 6, 8 biți
bits = [2, 4, 6, 8];
for b = bits
    quantized = max(min(round(file_resampled * (2^(b-1))) / (2^(b-1)), 1), -1);
    audiowrite(['pcm_', num2str(b), 'bits.wav'], quantized, new_fs);
    figure;
    plot(quantized);
    title(['Semnal PCM ', num2str(b), ' biți']); xlabel('Timp'); ylabel('Amplitudine');
end

% 4. Codare cu legea Miu
mu = 100; % Parametru Miu ales
compressed = sign(file_resampled) .* log(1 + mu * abs(file_resampled)) / log(1 + mu);
compressed = max(min(compressed, 1), -1); % Clipping
for b = bits
    quantized = max(min(round(compressed * (2^(b-1))) / (2^(b-1)), 1), -1);
    audiowrite(['mu_', num2str(mu), '_', num2str(b), 'bits.wav'], quantized, new_fs);
    figure;
    plot(quantized);
    title(['Legea Miu (', num2str(mu), ') ', num2str(b), ' biți']); xlabel('Timp'); ylabel('Amplitudine');
end

% 5. Codare cu legea A
A = randi([1, 100]); % Parametru A ales aleator
compressed_A = sign(file_resampled) .* log(1 + A * abs(file_resampled)) / log(1 + A);
compressed_A = max(min(compressed_A, 1), -1); % Clipping
for b = bits
    quantized = max(min(round(compressed_A * (2^(b-1))) / (2^(b-1)), 1), -1);
    audiowrite(['A_', num2str(A), '_', num2str(b), 'bits.wav'], quantized, new_fs);
    figure;
    plot(quantized);
    title(['Legea A (', num2str(A), ') ', num2str(b), ' biți']); xlabel('Timp'); ylabel('Amplitudine');
end

% 6. Modulația Delta
delta = 0.1; % Parametru Delta ajustabil
encoded = diff([0; file_resampled]);
quantized = max(min(round(encoded / delta) * delta, 1), -1);
decoded = cumsum(quantized);
decoded = max(min(decoded, 1), -1); % Clipping
audiowrite('delta_modulation.wav', decoded, new_fs);
figure;
plot(decoded);
title('Modulația Delta'); xlabel('Timp'); ylabel('Amplitudine');

% 7. Modulația Delta Adaptivă
gamma = 1.5; % Parametru Gamma ajustabil
adaptive_delta = delta;
adaptive_quantized = zeros(size(file_resampled));
for i = 2:length(file_resampled)
    diff_signal = file_resampled(i) - file_resampled(i-1);
    adaptive_quantized(i) = max(min(round(diff_signal / adaptive_delta) * adaptive_delta, 1), -1);
    adaptive_delta = adaptive_delta * gamma;
end
adaptive_decoded = cumsum(adaptive_quantized);
adaptive_decoded = max(min(adaptive_decoded, 1), -1); % Clipping
audiowrite('adaptive_delta_modulation.wav', adaptive_decoded, new_fs);
figure;
plot(adaptive_decoded);
title('Modulația Delta Adaptivă'); xlabel('Timp'); ylabel('Amplitudine');
