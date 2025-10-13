% 1 - Determinarea și trasarea unei curbe izofone folosind metoda ajustării în MATLAB by Vlad Popescu Gabriel

clear all

% Setarea inițială a parametrilor

nivel_foni = 40; % Nivelul în foni pentru audiție
frecventa_referinta = 1000; % Frecvența de referință la 1 kHz
frecvente_test = [125, 250, 500, 1000, 2000, 4000, 8000]; % Frecvențele de test
niveluri_dB_SPL = zeros(1, length(frecvente_test)); % Vector pentru nivelurile SPL

% Bucla pentru ajustarea intensității fiecărei frecvențe de test

for i = 1:length(frecvente_test)
    frecventa_curenta = frecvente_test(i);
    nivel_initial_dB = 30; % Nivel inițial pentru ajustare
    nivel_final = ajustareNivel(frecventa_referinta, nivel_initial_dB, frecventa_curenta, nivel_foni);
    niveluri_dB_SPL(i) = nivel_final; % Salvarea rezultatului ajustat
end

% Trasarea curbei izofone

figure;
plot(frecvente_test, niveluri_dB_SPL, '-o', 'LineWidth', 1.5);
xlabel('Frecvență (Hz)');                                               % Frecvențele testate, în Hz.
ylabel('Nivel dB SPL');                                                 % Nivelurile dB SPL ajustate, care arată intensitatea necesară pentru a obține aceeași tărie percepută ca referința de 1 kHz.
title(['Curba izofonă pentru ', num2str(nivel_foni), ' foni']);         % Graficul va avea o formă tipică a curbelor izofone, cu o sensibilitate maximă în jurul frecvenței de 1 kHz.
grid on;

% Salvarea graficului curbei izofone

saveas(gcf, 'Curba_Izofona_met_ajustarii_PVG.png');                                   % (get current figure) ia figura curentă activă.specifică numele fișierului și formatul de imagine.

% Funcția de ajustare a nivelului pentru metoda ajustării

function nivel_final = ajustareNivel(frecventa_ref, nivel_init, frecventa_test, foni)

                                 % Funcția `ajustareNivel` ajustează nivelul dB SPL al fiecărei frecvențe de test, modificându-l până când tăria percepută este echivalentă cu tăria tonului de referință de 1 kHz. 
    nivel = nivel_init;
    perceptie_ref = calculeazaTarie(frecventa_ref, foni);           % Tăria percepută pentru frecvența de referință
    perceptie_test = calculeazaTarie(frecventa_test, nivel);
    
    % Ajustare până când percepția devine egală. Crește sau scade nivelul dB SPL al frecvenței de test până când tăria percepută devine aproximativ egală cu cea de referință.

    while abs(perceptie_ref - perceptie_test) > 0.5
        if perceptie_test < perceptie_ref
            nivel = nivel + 1;                                      % Creștem nivelul dacă percepția este mai mică
        else
            nivel = nivel - 1;                                      % Scadem nivelul dacă percepția este mai mare
        end
        perceptie_test = calculeazaTarie(frecventa_test, nivel);    % Recalculăm percepția
    end
    nivel_final = nivel;                    
end

% Funcție auxiliară pentru calculul tăriei percepute

function tarie = calculeazaTarie(frecventa, nivel_dB)               % Aceasta calculează tăria percepută a unui sunet, având în vedere frecvența și nivelul dB SPL.

    % Formula ipotetică de calcul a tăriei percepute

    tarie = nivel_dB / (1 + abs(log10(frecventa / 1000)));
end
