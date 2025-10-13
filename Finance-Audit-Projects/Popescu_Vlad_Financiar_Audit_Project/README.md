			Proiect Python: Audit Financiar Automatizat by Popescu Vlad Gabriel

	Acest proiect oferă un sistem complet de audit financiar realizat în Python, destinat testării și analizării conturilor esențiale dintr-o firmă: cash, furnizori, clienți, costuri, cheltuieli și imobilizări.

	Ce face aplicația:

- Încarcă fișiere financiare (CSV)
- Rulează audit logic pe date reale
- Aplică eșantionare, verificări automate și agregări
- Generează fișier Excel + grafice
- Exportă document final `.docx` cu teorie + rezultate

	Structură

- `scripts/`: Codul sursă modular (clase, funcții)
- `data/`: Date simulate (CSV)
- `outputs/`: Rezultate salvate (Excel, grafice)
- `Proiect_Audit_Financiar.docx`: Raport final
- `requirements.txt`: Dependențe Python

	Tehnologii folosite

- Python 3
- Pandas, Matplotlib, Seaborn
- OpenPyXL, python-docx

	Rulare

1. Clonează repo:

```bash
git clone https://github.com/utilizator/audit-financiar-python.git
cd audit-financiar-python
```

2. Creează mediu virtual și instalează dependențele:

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Rulează scriptul:
```bash
python scripts/main.py
generate_charts.py      ```(Din rădăcina proiectului (audit_project), execută: python -m scripts.generate_charts)```
```

4. Verifică rezultatele în `outputs/`

	Licență

Proiect educațional – utilizare liberă.
Realizat de Popescu Vlad Gabriel

