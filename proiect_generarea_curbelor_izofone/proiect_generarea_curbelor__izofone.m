% Tema 3 - Script pentru generarea curbelor izofone conform modelului analitic
% Autor: © Vlad Gabriel Popescu

clear all;

% Parametri conform standardului ISO/IEC 226 (ediția a doua, 2003)
f = [20 25 31.5 40 50 63 80 100 125 160 200 250 315 400 500 630 800 1000 ...
    1250 1600 2000 2500 3150 4000 5000 6300 8000 10000 12500]; %[Hz]

% Parametri gamma și pragurile izofone
gamma = [0.532 0.506 0.480 0.455 0.432 0.409 0.387 0.367 0.349 0.330 0.315 ...
         0.301 0.288 0.276 0.267 0.259 0.253 0.250 0.246 0.244 0.243 0.243 ...
         0.243 0.242 0.242 0.245 0.254 0.271 0.301];

LTql = [78.5 68.7 59.5 51.1 44.0 37.5 31.5 26.5 22.1 17.9 14.4 11.4 8.4 5.8 ...
        3.8 2.1 1.0 0.8 3.5 1.7 -1.3 -4.2 -6.0 -5.4 -1.5 ...
        6.0 12.6 13.0 12.3 ]; % Prag de audibilitate în câmp liber [dB SPL]

LM = [-31.6 -27.2 -23.0 -19.1 -15.9 -13.0 -10.3 -8.1 -6.2 -4.5 -3.1 -2.0 -1.1 ...
     -0.4 0.0 0.3 0.5 0.0 -2.7 -4.1 -1.0 1.7 2.5 1.2 -2.1 -7.1 -11.2 -10.7 -3.1];%[dB]

% Introducerea parametrilor de utilizator
disp('Introduceți parametrii pentru generarea curbei izofone:');
Lref = input('Introduceți nivelul dorit în foni (ex: 40): ');
freq = input('Introduceți o frecvență dorită (Hz, ex: 1000): ');

% Validarea frecvenței
if freq < 20 || freq > 12500
    error('Frecvența introdusă trebuie să fie în intervalul [20, 12500] Hz.');
end

% Generare curba izofonă
curve = generateIsoCurve(Lref, freq, f, gamma, LTql, LM);

% Afișare rezultate
disp(['Curba izofonă generată pentru ', num2str(Lref), ' foni și frecvența dorită ', num2str(freq), ' Hz:']);
disp(table(f', curve', 'VariableNames', {'Frecvență (Hz)', 'Nivel (dB SPL)'}));

% Vizualizare grafică
figure;
semilogx(f, curve, '-o', 'LineWidth', 1.5);
title(['Curba izofonă pentru ', num2str(Lref), ' foni și frecvența dorită ', num2str(freq), ' Hz']);
xlabel('Frecvență [Hz]');
ylabel('Nivelul presiunii sonore [dB SPL]');
grid on;
set(gca, 'XScale', 'log');

% Funcția pentru calcularea curbelor izofone
function L = generateIsoCurve(Lref, freq, f, gamma, LTql, LM)
    % Lref: Nivelul dorit în foni
    % freq: Frecvența dorită în Hz
    % f: Vectorul frecvențelor
    % gamma: Parametrii gamma pentru fiecare frecvență
    % LTql: Pragul de audibilitate în câmp liber
    % LM: Parametrii funcției de transfer
    % L: Vectorul rezultat al nivelurilor presiunii sonore [dB SPL]
    
    L = zeros(size(f)); % Inițializare vector rezultat
    
    % Calculăm nivelul presiunii sonore pentru fiecare frecvență
    for i = 1:length(f)
        % Calculăm factorul A conform ecuației analitice
        A = 4.47 * 10^(-3) * (10^(0.025 * Lref) - 1.15) + ...
            (4 * 10^((LM(i) + LTql(i))/10 - 10))^gamma(i);
        
        % Calculăm nivelul presiunii sonore pentru curba izofonă
        L(i) = 94 - LM(i) + 10 * log10(A) / gamma(i);
    end
end
