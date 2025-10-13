
# Bookmarks – Scenarii uzuale (Promo ON/OFF, Canal, Regiune, Pareto)

## Cum creezi un bookmark
1. Aranjează pagina cu **slicerele** și **filtrele** dorite.
2. View → **Bookmarks** pane → **Add**. Bifează **Data** (ca să salveze starea filtrelor/slicerelor) și **Display**.
3. Rename cu un nume clar (ex.: `Promo ON`).
4. (Opțional) Creează un **Buttons → Bookmark navigator** pentru o bară de tab-uri.

## Seturi recomandate
- **Promo ON**: `dim_campaign[IsPromo] = TRUE` (filtru pe pagină sau slicer). Salvează ca bookmark.
- **Promo OFF**: `dim_campaign[IsPromo] = FALSE`.
- **Channel: Online**: `dim_channel[Channel] = "Online"`.
- **Region: București**: `dim_region[Region] = "București"`.
- **Pareto: Top 80%**: Adaugă un slicer pe măsura `Pareto 80% Flag` sau un filtru pe vizual `Pareto 80% Mask = 1`, apoi salvează bookmark.

## Sfaturi
- Pentru „Reset”: fă un bookmark **Clear filters** cu toți slicerii pe „All”.
- În Bookmark navigator, ordonează: `Reset | All Data | Promo ON | Promo OFF | Online | București | Pareto 80`.
- Dacă folosești **Sync slicers**, bookmark-urile vor aplica starea sincronizată și pe celelalte pagini (atenție la scoping).
