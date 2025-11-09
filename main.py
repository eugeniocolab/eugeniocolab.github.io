import time
import sys
from typing import List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd


URLS = [
    "https://leghe.fantacalcio.it/fantavvale25/classifica?id=596061",
    "https://leghe.fantacalcio.it/fantavvale25/classifica?id=596323",
    "https://leghe.fantacalcio.it/fantavvale25/classifica?id=596716",
    "https://leghe.fantacalcio.it/fantavvale25/classifica?id=596924",
]

# Se lasci vuoto => tutte le squadre. Se vuoi filtrare, metti i nomi qui.
TARGET_TEAMS= ["Pisa pi curt", "Real Forward"]

OUTPUT_XLSX = "classifica_storico.xlsx"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/127.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}


def normalize_name(name: str) -> str:
    return " ".join(name.split()).strip().lower()


def parse_italian_number(s: str) -> float:
    """
    Converte stringhe tipo '1.234,56' -> 1234.56
    """
    s = s.replace("\xa0", " ").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    return float(s)


def fetch_html(session: requests.Session, url: str, retries: int = 3, delay: float = 1.0) -> str:
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 200 and resp.text:
                return resp.text
            else:
                last_exc = RuntimeError(f"HTTP {resp.status_code} su {url}")
        except Exception as e:
            last_exc = e
        time.sleep(delay)
    raise last_exc


def parse_rankings(html: str, url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tbody tr.ranking-row")
    results = []

    for row in rows:
        team_td = row.select_one('td[data-key="teamName"]')
        points_td = row.select_one('td[data-key="rank-fp"]')  # "pt totali"
        pos_td = row.select_one('td[data-key="index"] span')

        if not team_td or not points_td:
            continue

        a = team_td.select_one("a")
        if a and a.get_text(strip=True):
            team_name = a.get_text(strip=True)
        else:
            for badge in team_td.select(".badge, .badge-bonusmalus"):
                badge.extract()
            team_name = team_td.get_text(" ", strip=True)

        points_text = points_td.get_text(" ", strip=True)
        if not points_text:
            continue

        try:
            points = parse_italian_number(points_text)
        except Exception:
            continue

        try:
            pos = int(pos_td.get_text(strip=True)) if pos_td else None
        except Exception:
            pos = None

        results.append({
            "Squadra": team_name,
            "Punti": points,
            "posizione": pos,
            "url": url,
        })

    return results


def collect_current_scores(all_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Deduplica per Squadra normalizzata prendendo il punteggio massimo.
    Ritorna: {squadra_norm: {"squadra": nome_originale, "Punti": float}}
    """
    scores: Dict[str, Dict[str, Any]] = {}
    for row in all_rows:
        norm = normalize_name(row["Squadra"])
        if norm not in scores or row["Punti"] > scores[norm]["Punti"]:
            scores[norm] = {"Squadra": row["Squadra"], "Punti": float(row["Punti"])}
    return scores


def apply_target_filter(scores: Dict[str, Dict[str, Any]], targets: List[str]) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
    if not targets:
        return scores, []
    target_norms = {normalize_name(t) for t in targets}
    filtered = {norm: v for norm, v in scores.items() if norm in target_norms}
    not_found = [t for t in targets if normalize_name(t) not in filtered.keys()]
    return filtered, not_found


def load_storico(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["giornata", "data_download", "Squadra", "squadra_norm", "punteggio_totale"])
    try:
        df = pd.read_excel(path, sheet_name="Storico")
        if "data_download" in df.columns:
            df["data_download"] = pd.to_datetime(df["data_download"])
        return df
    except Exception:
        # Se il file esiste ma non ha il foglio "Storico", riparti pulito
        return pd.DataFrame(columns=["giornata", "data_download", "Squadra", "squadra_norm", "Punti"])


def compute_prev_giornata_date(df_storico: pd.DataFrame, giornata: int, fallback_dt: datetime) -> datetime:
    try:
        prev_dt = df_storico.loc[df_storico["giornata"] == giornata, "data_download"].max()
        if pd.isna(prev_dt):
            return fallback_dt
        return pd.to_datetime(prev_dt)
    except Exception:
        return fallback_dt


def update_storico(
    df_storico: pd.DataFrame,
    current_scores: Dict[str, Dict[str, Any]],
    run_dt: datetime,
    float_eps: float = 1e-6
) -> Tuple[pd.DataFrame, int, Dict[str, int]]:
    """
    Regole:
    - Se non c'è ancora storico -> giornata = 1 con tutte le squadre.
    - Se c'è almeno una squadra con punteggio cambiato -> nuova giornata (giornata+1), aggiungendo solo le squadre cambiate.
      Squadre nuove: aggiunte alla giornata precedente con il punteggio attuale (backfill).
    - Se non cambia nessuno ma ci sono squadre nuove: aggiunte alla giornata precedente. Nessuna nuova giornata.
    - Se non cambia nessuno e non ci sono squadre nuove: nessuna modifica.

    Ritorna: (df_aggiornato, giornata_creata_o_None, stats)
    """
    stats = {"added_changed": 0, "added_new_prev": 0, "skipped_unchanged": 0, "no_change": 0}

    # Primo inserimento
    if df_storico.empty:
        rows = []
        for norm, obj in current_scores.items():
            rows.append({
                "giornata": 1,
                "data_download": run_dt,
                "Squadra": obj["Squadra"],
                "squadra_norm": norm,
                "punteggio_totale": obj["Punti"],
            })
        df_new = pd.DataFrame(rows)
        return df_new, 1, {"added_changed": len(rows), "added_new_prev": 0, "skipped_unchanged": 0, "no_change": 0}

    # Snapshot ultimo punteggio per squadra
    df_last = (
        df_storico.sort_values(["squadra_norm", "giornata"])
        .groupby("squadra_norm")
        .tail(1)
        .set_index("squadra_norm")
    )

    last_giornata = int(df_storico["giornata"].max())

    changed, unchanged, new_teams = [], [], []
    for norm, obj in current_scores.items():
        cur = float(obj["Punti"])
        if norm not in df_last.index:
            new_teams.append(norm)
        else:
            prev = float(df_last.loc[norm, "punteggio_totale"])
            if abs(cur - prev) > float_eps:
                changed.append(norm)
            else:
                unchanged.append(norm)

    rows_to_add = []

    if changed:
        # nuova giornata
        giornata_new = last_giornata + 1

        # aggiungo solo le squadre cambiate alla nuova giornata
        for norm in changed:
            obj = current_scores[norm]
            rows_to_add.append({
                "giornata": giornata_new,
                "data_download": run_dt,
                "Squadra": obj["Squadra"],
                "squadra_norm": norm,
                "punteggio_totale": obj["Punti"],
            })

        # backfill delle squadre nuove alla giornata precedente
        prev_dt = compute_prev_giornata_date(df_storico, last_giornata, run_dt)
        for norm in new_teams:
            obj = current_scores[norm]
            rows_to_add.append({
                "giornata": last_giornata,
                "data_download": prev_dt,
                "Squadra": obj["Squadra"],
                "squadra_norm": norm,
                "punteggio_totale": obj["Punti"],
            })

        stats["added_changed"] = len(changed)
        stats["added_new_prev"] = len(new_teams)
        stats["skipped_unchanged"] = len(unchanged)

        df_new = pd.DataFrame(rows_to_add)

        # evita duplicati (giornata, squadra_norm) se per qualche motivo già esistenti
        if not df_new.empty:
            key_cols = ["giornata", "squadra_norm"]
            existing_keys = set(tuple(x) for x in df_storico[key_cols].itertuples(index=False, name=None))
            mask = [ (r["giornata"], r["squadra_norm"]) not in existing_keys for _, r in df_new.iterrows() ]
            df_new = df_new[mask]

        df_out = pd.concat([df_storico, df_new], ignore_index=True) if not df_new.empty else df_storico.copy()
        return df_out, giornata_new, stats

    # Nessuna squadra è cambiata
    if new_teams:
        # aggiungo SOLO nuove squadre alla giornata precedente (nessuna nuova giornata)
        prev_dt = compute_prev_giornata_date(df_storico, last_giornata, run_dt)
        for norm in new_teams:
            obj = current_scores[norm]
            rows_to_add.append({
                "giornata": last_giornata,
                "data_download": prev_dt,
                "Squadra": obj["Squadra"],
                "squadra_norm": norm,
                "punteggio_totale": obj["Punti"],
            })
        stats["added_changed"] = 0
        stats["added_new_prev"] = len(new_teams)
        stats["skipped_unchanged"] = len(unchanged)
        df_new = pd.DataFrame(rows_to_add)

        # evita duplicati
        if not df_new.empty:
            key_cols = ["giornata", "squadra_norm"]
            existing_keys = set(tuple(x) for x in df_storico[key_cols].itertuples(index=False, name=None))
            mask = [ (r["giornata"], r["squadra_norm"]) not in existing_keys for _, r in df_new.iterrows() ]
            df_new = df_new[mask]

        df_out = pd.concat([df_storico, df_new], ignore_index=True) if not df_new.empty else df_storico.copy()
        return df_out, None, stats

    # Nessuna novità
    stats["no_change"] = 1
    stats["skipped_unchanged"] = len(unchanged)
    return df_storico.copy(), None, stats


def build_punteggi_giornata(df_storico: pd.DataFrame) -> pd.DataFrame:
    if df_storico.empty:
        return pd.DataFrame(columns=["giornata", "data_download", "Squadra", "punteggio_giornata"])
    df = df_storico.sort_values(["squadra_norm", "giornata"]).copy()
    df["prev_tot"] = df.groupby("squadra_norm")["punteggio_totale"].shift(1)
    df["punteggio_giornata"] = df["punteggio_totale"] - df["prev_tot"]
    out = df[df["prev_tot"].notna()][["giornata", "data_download", "Squadra", "punteggio_giornata"]].copy()
    out = out.sort_values(["giornata", "punteggio_giornata"], ascending=[True, False]).reset_index(drop=True)
    return out


def build_ultima_classifica(df_storico: pd.DataFrame) -> pd.DataFrame:
    if df_storico.empty:
        return pd.DataFrame(columns=["posizione", "Squadra", "punteggio_totale", "giornata", "data_download"])
    # ultimo record per squadra (potrebbe non essere la stessa giornata per tutti)
    last_per_team = (
        df_storico.sort_values(["squadra_norm", "giornata"])
        .groupby("squadra_norm")
        .tail(1)
        .copy()
    )
    last_per_team = last_per_team[["Squadra", "punteggio_totale", "giornata", "data_download"]]
    last_per_team = last_per_team.sort_values("punteggio_totale", ascending=False).reset_index(drop=True)
    last_per_team.insert(0, "posizione", range(1, len(last_per_team) + 1))
    return last_per_team


def save_excel(path: Path, df_storico: pd.DataFrame):
    df_storico_sorted = df_storico.sort_values(["giornata", "punteggio_totale"], ascending=[True, False])
    df_punti_giornata = build_punteggi_giornata(df_storico_sorted)
    df_ultima = build_ultima_classifica(df_storico_sorted)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df_storico_sorted.to_excel(writer, index=False, sheet_name="Storico")
        df_punti_giornata.to_excel(writer, index=False, sheet_name="Punteggi giornata")
        df_ultima.to_excel(writer, index=False, sheet_name="Ultima classifica")


def main():
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    all_rows: List[Dict[str, Any]] = []

    for url in URLS:
        try:
            html = fetch_html(session, url)
            page_rows = parse_rankings(html, url)
            all_rows.extend(page_rows)
            time.sleep(0.8)
        except Exception as e:
            print(f"Errore su {url}: {e}", file=sys.stderr)

    if not all_rows:
        print("Nessun dato estratto. La pagina potrebbe richiedere login o protezioni anti-bot.")
        sys.exit(1)

    current_scores = collect_current_scores(all_rows)

    # Opzionale: filtro squadre target
    filtered_scores, not_found = apply_target_filter(current_scores, TARGET_TEAMS)
    if TARGET_TEAMS:
        if not_found:
            print(f"Attenzione: non trovate le seguenti squadre target: {not_found}")
        current_scores = filtered_scores

    if not current_scores:
        print("Dopo il filtro, nessuna squadra disponibile. Esco.")
        sys.exit(0)

    run_dt = datetime.now()
    out_path = Path(OUTPUT_XLSX)
    df_storico = load_storico(out_path)

    df_updated, giornata_created, stats = update_storico(df_storico, current_scores, run_dt)

    if giornata_created is None and stats.get("added_new_prev", 0) == 0 and stats.get("no_change", 0) == 1:
        print("Nessuna variazione rispetto all'ultimo run e nessuna squadra nuova. Non aggiorno l'Excel.")
        sys.exit(0)

    save_excel(out_path, df_updated)

    # Log sintetico
    if giornata_created:
        print(f"Aggiunta nuova giornata: {giornata_created}")
    if stats.get("added_changed", 0):
        print(f"Squadre aggiornate (punteggio cambiato): {stats['added_changed']}")
    if stats.get("added_new_prev", 0):
        print(f"Squadre nuove (backfill sulla giornata precedente): {stats['added_new_prev']}")
    print(f"Excel aggiornato: {out_path.resolve()}")


if __name__ == "__main__":

    main()
