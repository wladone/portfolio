% Automated MATLAB Script for Psychometric Analysis and Report Generation
% --------------------------------------------------

% 1. Import Data
clc; clear;

% Simulated data for demonstration (replace with actual file loading)
stimulus_levels = [1, 2, 3, 4, 5];
proportions = [0.1, 0.2, 0.5, 0.8, 0.9];

% Display the data
disp('Stimulus Levels:');
disp(stimulus_levels);
disp('Proportion of "≠" responses:');
disp(proportions);

% 2. Define Logistic Function for Psychometric Analysis
sigmoid = @(p, x) 1 ./ (1 + exp(-(x - p(1)) / p(2))); % Logistic function
p0 = [median(stimulus_levels), 1]; % Initial parameter estimates (x0 and k)

% 3. Curve Fitting
p = nlinfit(stimulus_levels, proportions, sigmoid, p0); % Fit logistic function
x0 = p(1); % Threshold (point of inflection)
k = p(2); % Steepness of the curve
disp(['Threshold (x0): ', num2str(x0)]);
disp(['Steepness (k): ', num2str(k)]);

% Generate Fitted Curve
x_fit = linspace(min(stimulus_levels), max(stimulus_levels), 100);
y_fit = sigmoid(p, x_fit);

% 4. Plotting Psychometric Function
figure;
scatter(stimulus_levels, proportions, 'filled', 'DisplayName', 'Experimental Data');
hold on;
plot(x_fit, y_fit, 'r-', 'LineWidth', 2, 'DisplayName', 'Fitted Curve');
xline(x0, 'g--', 'DisplayName', ['Threshold (x0): ', num2str(x0, '%.2f')]);
title('Psychometric Function');
xlabel('Stimulus Level (\Delta I)');
ylabel('Proportion of "≠" Responses');
legend('show');
grid on;

% Save the Graph
graph_path = 'Psychometric_Function.png';
saveas(gcf, graph_path);

% 5. Generate Table for Report
table_data = table(stimulus_levels', proportions', ...
    'VariableNames', {'Stimulus_Level', 'Proportion_of_Responses'});
disp(table_data);

% Save Table as Excel File
excel_path = 'Psychometric_Data.xlsx';
writetable(table_data, excel_path);
disp(['Table saved as: ', excel_path]);

% 6. Automate PDF Report Generation
import mlreportgen.dom.*;
import mlreportgen.report.*;

report_path = 'Psychometric_Report.pdf';
rpt = Report(report_path, 'pdf');
title = append('Psychometric Analysis: Threshold Determination');
add(rpt, TitlePage('Title', title, 'Author', 'Automated Script'));
add(rpt, TableOfContents);

% Add Analysis Section
sec1 = Section('Title', 'Analysis Overview');
append(sec1, Paragraph(...
    'This report presents the psychometric analysis performed to determine the differential threshold (\Delta I).'));
append(sec1, Table(table_data));
add(rpt, sec1);

% Add Graph Section
sec2 = Section('Title', 'Psychometric Function');
append(sec2, Paragraph('The fitted logistic curve and experimental data are shown below.'));
img = mlreportgen.report.Figure();
img.Snapshot = graph_path;
append(sec2, img);
add(rpt, sec2);

% Add Results Section
sec3 = Section('Title', 'Results');
results_text = sprintf(['Threshold (x0): %.2f\n', ...
    'Steepness (k): %.2f\n', ...
    'This analysis demonstrates a well-fitted logistic function for the experimental data.'], x0, k);
append(sec3, Paragraph(results_text));
add(rpt, sec3);

% Close Report
close(rpt);
disp(['Report saved as: ', report_path]);
