clear all;
close all;
clc;

M=44; 
N=24;
L=M+N;
n=1:L;
x=0.5*cos(2*pi/12*n);
rxx=zeros(1,M);

fid=fopen('C:\Users\vladp\Desktop\441G__Popescu_Vlad_Gabriel\441G_IA_Popescu_Vlad_Gabriel\test\testx.dat','w','b');
fwrite(fid,x.*2^15,'int16');  % printeaza in fisierul descris mai sus valorile vectorului x; inmultim valorile deoarece vrem sa folosim numere intregi cand vom folosi CodeWarrior
fclose(fid);

for j=1:M       % calculul vectorului de corelatie cu formula data
    suma=0;
    for k=1:N
        suma=suma+x(k)*x(k+j);
    end
    rxx(j)=suma;
end
rxx

%%%%%%%%%%%%%%%%%%%%%%%

fid=fopen('C:\Users\vladp\Desktop\441G__Popescu_Vlad_Gabriel\441G_IA_Popescu_Vlad_Gabriel\test\result.dat','r','b');
%d=fscanf(fid,'%d')  %  specify floating-point numbers.
d=fscanf(fid,'%d');  % citirea din fisierul generat din CodeWarrior
d=d/(2^15);  % impartim la valoare la puterea cu care am inmultit initial, deoarece aceste valori au fost obtinute din inmultirea a doua elemente din sirurile de valori de test initiale
length(d)
fclose(fid);  % inchidere fisier
e=d-rxx' % eroarea – diferenta dintre valorile obtinute folosind direct Matlab sau CodeWarrior
Num=1:44;
figure
subplot(3,1,1);
plot(Num,rxy)
ylim([-50 50])
title('Autocorelatie - Matlab')
subplot(3,1,2); 
plot(Num,d)
ylim([-50 50])
title('Autocorelatie - CodeWarrior')
subplot(3,1,3); 
plot(Num,e)
ylim([-50 50])
title('Eroare')

