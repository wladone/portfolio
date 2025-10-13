% Citirea și procesarea datelor experimentale
auditii = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]; % Exemplu de date
raspunsuri = [0, 1, 1, 1, 0, 0, 1, 1, 0, 1]; % Decizii experimentale

% Calcularea proporției de decizii "!="
proportii = sum(raspunsuri) / length(raspunsuri);

% Ajustarea curbei psihometrice
x = linspace(0, 1, 100); % Intervalul diferenței de intensitate
x0 = 0.5; % Punctul de inflexiune
k = 2; % Parametrul de panta
y = 1 ./ (1 + exp(-(x - x0) * k)); % Funcția logistică

% Reprezentare grafică
figure;
plot(x, y, 'r-', 'LineWidth', 2);
hold on;
scatter(auditii, raspunsuri, 'bo', 'filled');
xlabel('Diferența de intensitate');
ylabel('Proporția de răspunsuri "!="');
title('Curba psihometrică ajustată');
grid on;
