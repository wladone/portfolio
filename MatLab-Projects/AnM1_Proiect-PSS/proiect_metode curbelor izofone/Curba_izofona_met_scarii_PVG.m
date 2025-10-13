%  - Determinarea și trasarea unei curbe izofone folosind metoda scarii în MATLAB by Vlad Popescu Gabriel

clear all

% Setări inițiale

nivel_foni = 50;                    % Am ales un nivel de 50 foni pentru un confort auditiv standard. Frecvențele testate sunt de la 125 Hz la 8000 Hz, acoperind intervalul de interes în psihoacustică
frecventa_referinta = 1000;         % Frecvența de referință la 1 kHz
frecvente_test = [125, 250, 500, 1000, 2000, 4000, 8000];       % Frecvențe de test
niveluri_dB_SPL = zeros(1, length(frecvente_test));             % Vector pentru nivelurile SPL.Creează un vector gol în care vei stoca nivelurile finale ajustate. Fiecare valoare din acest vector va corespunde unui nivel de dB SPL pentru o anumită frecvență, obținută prin metoda ajustării

% Bucla de test pentru fiecare frecvență

for i = 1:length(frecvente_test)
    frecventa_curenta = frecvente_test(i);
    nivel_initial_dB = 30;                       % Nivel inițial aproximativ pentru fiecare frecvență.
    nivel_final = ajustareNivel(frecventa_referinta, nivel_initial_dB, frecventa_curenta, nivel_foni);
    niveluri_dB_SPL(i) = nivel_final;            % În fiecare iterație, folosim funcția `ajustareNivel` pentru a ajusta nivelul dB SPL. Rezultatul final este stocat în `niveluri_dB_SPL` pentru a reprezenta punctele curbei izofone.
end

% Trasarea curbei izofone

figure;
plot(frecvente_test, niveluri_dB_SPL, '-o', 'LineWidth', 1.5);
xlabel('Frecvență (Hz)');                                       % reprezintă frecvențele, în Hz.
ylabel('Nivel dB SPL');                                         % reprezintă nivelul de presiune sonoră în dB SPL.
title(['Curba izofonă prin metoda scarii pentru ', num2str(nivel_foni), ' foni']);
grid on;

% Salvarea rezultatului

saveas(gcf, 'Curba_Izofona_met_scarii_PVG.png');

% Funcție pentru ajustarea nivelului SPL pentru metoda scarii

function nivel_final = ajustareNivel(frecventa_ref, nivel_init, frecventa_test, foni)

    % Aceasta funcție ajustează nivelul dB SPL al sunetului de test până când tăria percepută este echivalentă cu cea de referință. Când nivelul sunetului de test este egal perceput cu cel al referinței, funcția returnează nivelul.

    nivel = nivel_init;
    perceptie_ref = calculeazaTarie(frecventa_ref, foni); % Tăria percepută pentru referință
    perceptie_test = calculeazaTarie(frecventa_test, nivel);
    
    % Ajustare până când percepția este aproximativ egală

    while abs(perceptie_ref - perceptie_test) > 0.5
        if perceptie_test < perceptie_ref
            nivel = nivel + 1; % Creștem nivelul dacă este mai mic
        else
            nivel = nivel - 1; % Scădem nivelul dacă este mai mare
        end
        perceptie_test = calculeazaTarie(frecventa_test, nivel); % Recalculăm percepția
    end
    nivel_final = nivel;
end

% Funcție auxiliară pentru calculul tăriei percepute.Am implementat o funcție simplă pentru a aproxima tăria percepută la diferite frecvențe. În practică, funcția ar putea folosi o formulă specifică standardului ISO.

function tarie = calculeazaTarie(frecventa, nivel_dB)

    % Formula simplificată de calcul a tăriei percepute (în funcție de foni)

    tarie = nivel_dB / (1 + abs(log10(frecventa / 1000)));
end
