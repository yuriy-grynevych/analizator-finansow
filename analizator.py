import pandas as pd
import numpy as np
import requests
import re
import streamlit as st
import time
from datetime import date
from sqlalchemy import text
import io

# --- USTAWIENIA STRONY ---
st.set_page_config(page_title="Analizator Wydatków", layout="wide")

# --- KOD DO UKRYCIA STOPKI I MENU ---
hide_streamlit_style = """
            <style>
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

# --- PARAMETRY TABELI ---
NAZWA_TABELI = "transactions"
NAZWA_TABELI_PLIKOW = "saved_files"
NAZWA_SCHEMATU = "public"
NAZWA_POLACZENIA_DB = "db"

# --- SŁOWNIK VAT ---
VAT_RATES = {
    "PL": 0.23, "DE": 0.19, "CZ": 0.21, "AT": 0.20, "FR": 0.20,
    "DK": 0.25, "NL": 0.21, "BE": 0.21, "ES": 0.21, "IT": 0.22,
}

# --- LISTY DO PARSOWANIA PLIKU 'analiza.xlsx' ---
# Tutaj zmiana: Interesuje nas tylko Sprzedaż w kontekście analizy, 
# ale definicje zostawiam, żeby nie psuć logiki wykrywania wierszy.
ETYKIETY_PRZYCHODOW = ['Faktura VAT sprzedaży']
ETYKIETY_KOSZTOW_INNYCH = [
    'Faktura VAT zakupu', 'Korekta faktury VAT sprzedaży', 'Przychód wewnętrzny',
    'Korekta faktury VAT zakupu', 'Art. biurowe', 'Art. chemiczne', 'Art. spożywcze',
    'Delegacja', 'Giełda', 'Księgowość', 'Leasing', 'Mandaty', 'Obsługa prawna',
    'Ogłoszenie', 'Poczta Polska', 'Program', 'Prowizje', 'Serwis', 'Wykup auta',
    'Wysyłka kurierska', 'Zak. do auta', 'Zakup auta'
]
ETYKIETY_IGNOROWANE = [
    'Opłata drogowa', 'Opłata drogowa DK', 'Tankowanie', 'Suma końcowa', 'Nr pojazdu',
    'Zamówienie od klienta', 'Wydanie zewnętrzne'
]
WSZYSTKIE_ZNANE_ETYKIETY = ETYKIETY_PRZYCHODOW + ETYKIETY_KOSZTOW_INNYCH + ETYKIETY_IGNOROWANE

# --- FUNKCJE NBP (BEZ ZMIAN) ---
@st.cache_data
def pobierz_kurs_eur_pln():
    try:
        url = 'http://api.nbp.pl/api/exchangerates/rates/a/eur/?format=json'
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        kurs = response.json()['rates'][0]['mid']
        return kurs
    except requests.exceptions.RequestException as e:
        st.error(f"Nie udało się pobrać kursu EUR/PLN z NBP. Błąd: {e}")
        return None

@st.cache_data
def pobierz_kurs_do_pln(waluta_kod):
    waluta_kod = waluta_kod.lower()
    for tabela in ['a', 'b']:
        try:
            url = f'http://api.nbp.pl/api/exchangerates/rates/{tabela}/{waluta_kod}/?format=json'
            response = requests.get(url, timeout=5)
            if response.status_code == 404: continue
            response.raise_for_status()
            kurs = response.json()['rates'][0]['mid']
            return kurs
        except requests.exceptions.RequestException: pass
    return None

@st.cache_data
def pobierz_wszystkie_kursy(waluty_lista, kurs_eur_pln):
    mapa_kursow_do_eur = {'EUR': 1.0, 'PLN': 1.0 / kurs_eur_pln}
    waluty_do_pobrania = [w for w in waluty_lista if w not in mapa_kursow_do_eur and pd.notna(w)]
    for waluta in waluty_do_pobrania:
        time.sleep(0.1)
        kurs_pln = pobierz_kurs_do_pln(waluta)
        if kurs_pln: mapa_kursow_do_eur[waluta] = kurs_pln / kurs_eur_pln
        else: mapa_kursow_do_eur[waluta] = 0.0
    return mapa_kursow_do_eur

# --- KATEGORYZACJA TRANSAKCJI (BEZ ZMIAN - DOTYCZY PALIWA) ---
def kategoryzuj_transakcje(row, zrodlo):
    if zrodlo == 'Eurowag':
        usluga = str(row.get('Usługa', '')).upper()
        artykul = str(row.get('Artykuł', '')).strip()

        if 'TOLL' in usluga.upper() or 'OPŁATA DROGOWA' in usluga.upper():
            return 'OPŁATA', artykul
        if 'DIESEL' in artykul.upper() or 'ON' in artykul.upper():
            return 'PALIWO', 'Diesel'
        if 'ADBLUE' in artykul.upper():
            return 'PALIWO', 'AdBlue'
        if 'OPENLOOP' in usluga.upper() or 'VISA' in usluga.upper():
            return 'INNE', 'Płatność kartą'
        return 'INNE', artykul

    elif zrodlo == 'E100_PL':
        usluga = str(row.get('Usługa', '')).strip()
        kategoria = str(row.get('Kategoria', '')).upper()

        if 'TOLL' in usluga.upper() or 'OPŁATA DROGOWA' in usluga.upper():
            return 'OPŁATA', usluga
        if 'ON' in usluga.upper() or 'DIESEL' in kategoria:
            return 'PALIWO', 'Diesel'
        if 'ADBLUE' in usluga.upper() or 'ADBLUE' in kategoria:
            return 'PALIWO', 'AdBlue'
        return 'INNE', usluga

    elif zrodlo == 'E100_EN':
        service = str(row.get('Service', '')).strip()
        category = str(row.get('Category', '')).upper()

        if 'TOLL' in service.upper():
            return 'OPŁATA', service
        if 'DIESEL' in service.upper() or 'DIESEL' in category:
            return 'PALIWO', 'Diesel'
        if 'ADBLUE' in service.upper() or 'ADBLUE' in category:
            return 'PALIWO', 'AdBlue'
        return 'INNE', service

    return 'INNE', 'Nieznane'

# --- NORMALIZACJA DANYCH PALIWOWYCH (BEZ ZMIAN) ---
def normalizuj_eurowag(df_eurowag):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_eurowag['Data i godzina'], errors='coerce')
    df_out['identyfikator'] = df_eurowag['Tablica rejestracyjna'].fillna(df_eurowag['Posiadacz karty'].fillna(df_eurowag['Karta']))
    df_out['kwota_netto'] = pd.to_numeric(df_eurowag['Kwota netto'], errors='coerce')
    df_out['kwota_brutto'] = pd.to_numeric(df_eurowag['Kwota brutto'], errors='coerce')
    df_out['waluta'] = df_eurowag['Waluta']
    df_out['ilosc'] = pd.to_numeric(df_eurowag['Ilość'], errors='coerce')
    kategorie = df_eurowag.apply(lambda row: kategoryzuj_transakcje(row, 'Eurowag'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie]
    df_out['produkt'] = [kat[1] for kat in kategorie]
    df_out['zrodlo'] = 'Eurowag'
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

def normalizuj_e100_PL(df_e100):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_e100['Data'] + ' ' + df_e100['Czas'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    df_out['identyfikator'] = df_e100['Numer samochodu'].fillna(df_e100['Numer karty'])
    kwota_brutto = pd.to_numeric(df_e100['Kwota'], errors='coerce')
    vat_rate = df_e100['Kraj'].map(VAT_RATES).fillna(0.0)
    df_out['kwota_netto'] = kwota_brutto / (1 + vat_rate)
    df_out['kwota_brutto'] = kwota_brutto
    df_out['waluta'] = df_e100['Waluta']
    df_out['ilosc'] = pd.to_numeric(df_e100['Ilość'], errors='coerce')
    kategorie = df_e100.apply(lambda row: kategoryzuj_transakcje(row, 'E100_PL'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie]
    df_out['produkt'] = [kat[1] for kat in kategorie]
    df_out['zrodlo'] = 'E100_PL'
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

def normalizuj_e100_EN(df_e100):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_e100['Date'] + ' ' + df_e100['Time'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    df_out['identyfikator'] = df_e100['Car registration number'].fillna(df_e100['Card number'])
    kwota_brutto = pd.to_numeric(df_e100['Sum'], errors='coerce')
    vat_rate = df_e100['Country'].map(VAT_RATES).fillna(0.0)
    df_out['kwota_netto'] = kwota_brutto / (1 + vat_rate)
    df_out['kwota_brutto'] = kwota_brutto
    df_out['waluta'] = df_e100['Currency']
    df_out['ilosc'] = pd.to_numeric(df_e100['Quantity'], errors='coerce')
    kategorie = df_e100.apply(lambda row: kategoryzuj_transakcje(row, 'E100_EN'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie]
    df_out['produkt'] = [kat[1] for kat in kategorie]
    df_out['zrodlo'] = 'E100_EN'
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

# --- WCZYTYWANIE PLIKÓW (BEZ ZMIAN) ---
def wczytaj_i_zunifikuj_pliki(przeslane_pliki):
    lista_df_zunifikowanych = []
    for plik in przeslane_pliki:
        nazwa_pliku_base = plik.name
        st.write(f" - Przetwarzam: {nazwa_pliku_base}")
        try:
            if nazwa_pliku_base.endswith(('.xls', '.xlsx')):
                xls = pd.ExcelFile(plik, engine='openpyxl')
                if 'Transactions' in xls.sheet_names:
                    df_e100 = pd.read_excel(xls, sheet_name='Transactions')
                    if 'Numer samochodu' in df_e100.columns:
                        lista_df_zunifikowanych.append(normalizuj_e100_PL(df_e100))
                    elif 'Car registration number' in df_e100.columns:
                        lista_df_zunifikowanych.append(normalizuj_e100_EN(df_e100))
                elif len(xls.sheet_names) > 0:
                    df_eurowag = pd.read_excel(xls, sheet_name=0)
                    if 'Data i godzina' in df_eurowag.columns:
                        if 'Posiadacz karty' not in df_eurowag.columns: df_eurowag['Posiadacz karty'] = None
                        lista_df_zunifikowanych.append(normalizuj_eurowag(df_eurowag))
        except Exception as e:
           st.error(f"BŁĄD: {e}")
    
    if not lista_df_zunifikowanych: return None, "Błąd."
    return pd.concat(lista_df_zunifikowanych, ignore_index=True), None

# --- BAZA DANYCH (BEZ ZMIAN) ---
def setup_database(conn):
    with conn.session as s:
        s.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {NAZWA_SCHEMATU}.{NAZWA_TABELI} (
                id SERIAL PRIMARY KEY,
                data_transakcji TIMESTAMP,
                identyfikator VARCHAR(255),
                kwota_netto FLOAT,
                kwota_brutto FLOAT,
                waluta VARCHAR(10),
                ilosc FLOAT,
                produkt VARCHAR(255),
                typ VARCHAR(50),
                zrodlo VARCHAR(50)
            );
        """))
        s.commit()

def setup_file_database(conn):
    try:
        with conn.session as s:
            s.execute(text(f"DROP TABLE IF EXISTS {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW}"))
            s.commit()
            s.execute(text(f"""
                CREATE TABLE {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} (
                    file_name VARCHAR(255) PRIMARY KEY,
                    file_data BYTEA,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            s.commit()
        st.success(f"Tabela plików OK.")
    except Exception as e:
        st.error(f"BŁĄD: {e}")

def wyczysc_duplikaty(conn):
    with conn.session as s:
        s.execute(text(f"""
        DELETE FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI} a
        WHERE a.ctid <> (
            SELECT min(b.ctid)
            FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI} b
            WHERE a.data_transakcji = b.data_transakcji
              AND a.identyfikator = b.identyfikator
              AND a.kwota_brutto = b.kwota_brutto
              AND a.waluta = b.waluta
        );
        """))
        s.commit()

def pobierz_dane_z_bazy(conn, data_start, data_stop, typ=None):
    params = {"data_start": data_start, "data_stop": data_stop}
    query = f"""
        SELECT * FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}
        WHERE (data_transakcji::date) >= :data_start
          AND (data_transakcji::date) <= :data_stop
    """
    if typ:
        query += " AND typ = :typ"
        params["typ"] = typ
    df = conn.query(query, params=params)
    return df

def zapisz_plik_w_bazie(conn, file_name, file_bytes):
    try:
        if hasattr(file_bytes, 'getvalue'): file_bytes = file_bytes.getvalue()
        with conn.session as s:
            s.execute(text(f"DELETE FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} WHERE file_name = :name"), {"name": file_name})
            s.execute(text(f"INSERT INTO {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} (file_name, file_data) VALUES (:name, :data)"), {"name": file_name, "data": file_bytes})
            s.commit()
        st.success("Zapisano plik!")
        time.sleep(1)
        st.rerun()
    except Exception as e: st.error(f"BŁĄD: {e}")

def wczytaj_plik_z_bazy(conn, file_name):
    try:
        with conn.session as s:
            res = s.execute(text(f"SELECT file_data FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} WHERE file_name = :n"), {"n": file_name}).fetchone()
            if res: return res[0].tobytes() if isinstance(res[0], memoryview) else res[0]
    except: pass
    return None

def usun_plik_z_bazy(conn, file_name):
    with conn.session as s:
        s.execute(text(f"DELETE FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} WHERE file_name = :n"), {"n": file_name})
        s.commit()
    st.rerun()

def bezpieczne_czyszczenie_klucza(s_identyfikatorow):
    s_str = s_identyfikatorow.astype(str)
    def clean_key(key):
        if key == 'nan' or not key: return 'Brak Identyfikatora'
        key_nospace = key.upper().replace(" ", "").replace("-", "").strip().strip('"')
        BLACKLIST = ['TRUCK24SP', 'EDENRED', 'MARMAR', 'SANTANDER', 'LEASING', 'PZU', 'WARTA', 'INTERCARS', 'EUROWAG', 'E100']
        for b in BLACKLIST:
            if b in key_nospace: return 'Brak Identyfikatora'
        if key_nospace.startswith("PL") and len(key_nospace) > 7: key_nospace = key_nospace[2:]
        if not key_nospace: return 'Brak Identyfikatora'
        if key_nospace.startswith("("): return key 
        match = re.search(r'([A-Z0-9]{4,12})', key_nospace)
        if match:
            found = match.group(1)
            if found in BLACKLIST: return 'Brak Identyfikatora'
            return found
        if any(char.isdigit() for char in key_nospace): return key_nospace
        return 'Brak Identyfikatora'
    return s_str.apply(clean_key)

def przygotuj_dane_paliwowe(dane_z_bazy):
    if dane_z_bazy.empty: return dane_z_bazy, None
    dane_z_bazy['data_transakcji_dt'] = pd.to_datetime(dane_z_bazy['data_transakcji'])
    dane_z_bazy['identyfikator_clean'] = bezpieczne_czyszczenie_klucza(dane_z_bazy['identyfikator'])
    kurs = pobierz_kurs_eur_pln()
    if not kurs: return None, None
    mapa = pobierz_wszystkie_kursy(dane_z_bazy['waluta'].unique(), kurs)
    dane_z_bazy['kwota_netto_num'] = pd.to_numeric(dane_z_bazy['kwota_netto'], errors='coerce').fillna(0.0)
    dane_z_bazy['kwota_brutto_num'] = pd.to_numeric(dane_z_bazy['kwota_brutto'], errors='coerce').fillna(0.0)
    dane_z_bazy['kwota_netto_eur'] = dane_z_bazy.apply(lambda r: r['kwota_netto_num'] * mapa.get(r['waluta'], 0.0), axis=1)
    dane_z_bazy['kwota_brutto_eur'] = dane_z_bazy.apply(lambda r: r['kwota_brutto_num'] * mapa.get(r['waluta'], 0.0), axis=1)
    return dane_z_bazy, mapa

# --- GŁÓWNA MODYFIKACJA DLA TABELI RENTOWNOŚCI ---
@st.cache_data
def przetworz_plik_analizy(przeslany_plik_bytes, data_start, data_stop):
    MAPA_WALUT_PLIKU = {'euro': 'EUR', 'złoty polski': 'PLN', 'korona duńska': 'DKK'}
    TYP_BRUTTO = 'Suma Wartosc_BruttoPoRabacie'
    TYP_NETTO = 'Suma Wartosc_NettoPoRabacie'
    
    try:
        kurs_eur = pobierz_kurs_eur_pln()
        if not kurs_eur: return None, None
        mapa_kursow = pobierz_wszystkie_kursy(list(MAPA_WALUT_PLIKU.values()), kurs_eur)
        st.info(f"Kurs EUR/PLN: {kurs_eur:.4f}")
    except: return None, None
    
    try:
        df = pd.read_excel(przeslany_plik_bytes, sheet_name='pojazdy', engine='openpyxl', header=[7, 8])
        col_lbl = df.columns[0]
        
        MAPA_BRUTTO, MAPA_NETTO = {}, {}
        for c_wal, c_typ in df.columns:
            if c_wal in MAPA_WALUT_PLIKU:
                k = mapa_kursow.get(MAPA_WALUT_PLIKU[c_wal], 0.0)
                if MAPA_WALUT_PLIKU[c_wal] == 'EUR': k = 1.0
                if c_typ == TYP_BRUTTO: MAPA_BRUTTO[(c_wal, c_typ)] = k
                if c_typ == TYP_NETTO: MAPA_NETTO[(c_wal, c_typ)] = k
    except: return None, None

    wyniki = []
    aktualny_kontrahent = "Nieznany"
    aktualna_data = None
    date_regex = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    
    # Funkcja do wykrywania czy wiersz to identyfikator pojazdu
    def is_vehicle_line(line):
        if not line or line == 'nan': return False
        l = str(line).strip().upper()
        BLACKLIST = ['E100', 'EUROWAG', 'VISA', 'MASTER', 'ORLEN', 'LOTOS', 'BP', 'SHELL', 'UTA', 'DKV', 'PKO', 'SANTANDER', 'ING', 'TRUCK24', 'EDENRED', 'INTERCARS', 'MARMAR', 'LEASING', 'UBER', 'BOLT', 'SERWIS', 'POLSKA', 'SPOLKA', 'GROUP', 'LOGISTICS', 'TRANS', 'SYSTEM', 'ZAMÓWIENIE OD KLIENTA', 'FAKTURA VAT']
        if l in BLACKLIST: return False
        words = re.split(r'[\s+Ii]+', l)
        for w in words:
            if not w or len(w)<4: continue
            w = w.replace("-", "")
            if any(b in w for b in BLACKLIST): continue
            if (any(c.isalpha() for c in w) and any(c.isdigit() for c in w)) or (w.isdigit() and len(w)>=4): return True
        return False

    # Główna pętla
    last_vehicle_list = []

    for index, row in df.iterrows():
        try:
            etykieta = str(row[col_lbl]).strip()
            kw_brutto = 0.0
            kw_netto = 0.0
            for c, k in MAPA_BRUTTO.items(): 
                v = pd.to_numeric(row[c], errors='coerce')
                if pd.notna(v): kw_brutto += v * k
            for c, k in MAPA_NETTO.items():
                v = pd.to_numeric(row[c], errors='coerce')
                if pd.notna(v): kw_netto += v * k
            kw_total = kw_brutto if kw_brutto != 0 else kw_netto
        except: continue

        # 1. Data
        if isinstance(row[col_lbl], (pd.Timestamp, date)) or date_regex.match(etykieta):
            if isinstance(row[col_lbl], (pd.Timestamp, date)): aktualna_data = row[col_lbl].date()
            else: 
                try: aktualna_data = pd.to_datetime(etykieta).date()
                except: pass
            aktualny_kontrahent = "Nieznany"
            last_vehicle_list = []
            continue
        
        # 2. Wykrywanie kontekstu (Pojazd lub Kontrahent)
        if etykieta and etykieta != 'nan' and etykieta not in WSZYSTKIE_ZNANE_ETYKIETY:
            if is_vehicle_line(etykieta):
                parts = re.split(r'\s+i\s+|\s+I\s+|\s*\+\s*', etykieta)
                last_vehicle_list = [p.strip() for p in parts if p.strip()]
            else:
                # To jest nazwa firmy (Kontrahenta)
                if "Zamówienie" not in etykieta:
                    aktualny_kontrahent = etykieta.strip('"')
            continue

        # 3. Logika filtrowania - TYLKO 'Faktura VAT sprzedaży'
        # Ignorujemy wszystko co nie jest Fakturą Sprzedaży
        if etykieta == 'Faktura VAT sprzedaży':
             if kw_total != 0.0 and aktualna_data and data_start <= aktualna_data <= data_stop:
                 if not last_vehicle_list: continue # Ignoruj jeśli nie przypisano do auta
                 
                 split_brutto = kw_brutto / len(last_vehicle_list)
                 split_netto = kw_netto / len(last_vehicle_list)
                 
                 for veh in last_vehicle_list:
                     wyniki.append({
                         'data': aktualna_data,
                         'pojazd_oryg': veh,
                         'opis': etykieta,
                         'kontrahent': aktualny_kontrahent, # Dodano kontrahenta
                         'typ': 'Przychód (Subiekt)',
                         'zrodlo': 'Subiekt',
                         'kwota_brutto_eur': split_brutto,
                         'kwota_netto_eur': split_netto
                     })
        # Ignorujemy 'Faktura VAT zakupu', 'Korekta' itd.

    if not wyniki: return None, None
    
    df_wyniki = pd.DataFrame(wyniki)
    # Czyścimy śmieciowe wpisy
    BAD = ['TRUCK24SP', 'EDENRED', 'MARMAR', 'INTERCARS', 'SANTANDER', 'LEASING']
    for b in BAD: df_wyniki = df_wyniki[~df_wyniki['pojazd_oryg'].str.upper().str.contains(b, na=False)]
    
    df_wyniki['pojazd_clean'] = bezpieczne_czyszczenie_klucza(df_wyniki['pojazd_oryg'])
    df_wyniki = df_wyniki[df_wyniki['pojazd_clean'] != 'Brak Identyfikatora']
    
    # Agregacja dla tabeli rentowności
    df_przychody = df_wyniki.groupby('pojazd_clean')['kwota_brutto_eur'].sum().to_frame('przychody_brutto')
    df_przychody_netto = df_wyniki.groupby('pojazd_clean')['kwota_netto_eur'].sum().to_frame('przychody_netto')
    
    # Koszty z pliku są teraz zerowe, bo bierzemy tylko sprzedaż
    df_koszty = pd.DataFrame(0.0, index=df_przychody.index, columns=['koszty_inne_brutto'])
    df_koszty_netto = pd.DataFrame(0.0, index=df_przychody.index, columns=['koszty_inne_netto'])

    return pd.concat([df_przychody, df_przychody_netto, df_koszty, df_koszty_netto], axis=1).fillna(0), df_wyniki

# --- APLIKACJA ---
def main_app():
    st.title("Analizator Wydatków Floty")
    
    @st.cache_data
    def to_excel(df):
        o = io.BytesIO()
        with pd.ExcelWriter(o, engine='openpyxl') as w: df.to_excel(w, sheet_name='Raport')
        return o.getvalue()

    tab_admin, tab_raport, tab_rentownosc = st.tabs(["⚙️ Panel Admina", "📊 Raport Paliw/Opłat", "💰 Rentowność (Zysk/Strata)"])
    try: conn = st.connection(NAZWA_POLACZENIA_DB, type="sql")
    except: st.error("Błąd połączenia z bazą."); st.stop()

    # TAB 1
    with tab_admin:
        st.header("Panel Admina")
        c1, c2 = st.columns(2)
        if c1.button("1. Stwórz bazę transakcji"): setup_database(conn)
        if c2.button("2. Stwórz bazę plików"): setup_file_database(conn)
        st.divider()
        files = st.file_uploader("Pliki E100/Eurowag", accept_multiple_files=True)
        if files and st.button("Wgraj"):
            d, e = wczytaj_i_zunifikuj_pliki(files)
            if d is not None:
                d.to_sql(NAZWA_TABELI, conn.engine, if_exists='append', index=False, schema=NAZWA_SCHEMATU)
                wyczysc_duplikaty(conn)
                st.success("OK")
            else: st.error(e)

    # TAB 2 - BEZ ZMIAN
    with tab_raport:
        st.header("Raport Paliw i Opłat")
        try:
            mm = conn.query(f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}")
            if not mm.empty and mm.iloc[0,0]:
                d1 = st.date_input("Start", mm.iloc[0,0], key='d1')
                d2 = st.date_input("Stop", mm.iloc[0,1], key='d2')
                df_baza = pobierz_dane_z_bazy(conn, d1, d2)
                df_baza, _ = przygotuj_dane_paliwowe(df_baza)
                
                if df_baza is not None:
                    t1, t2, t3 = st.tabs(["Paliwo", "Opłaty", "Inne"])
                    with t1:
                        p = df_baza[df_baza['typ']=='PALIWO']
                        agg = p.groupby('identyfikator_clean')['kwota_brutto_eur'].sum().sort_values(ascending=False)
                        st.dataframe(agg)
                    with t2:
                        o = df_baza[df_baza['typ']=='OPŁATA']
                        agg = o.groupby('identyfikator_clean')['kwota_brutto_eur'].sum().sort_values(ascending=False)
                        st.dataframe(agg)
                    with t3:
                        i = df_baza[df_baza['typ']=='INNE']
                        agg = i.groupby('identyfikator_clean')['kwota_brutto_eur'].sum().sort_values(ascending=False)
                        st.dataframe(agg)
        except Exception as e: st.error(f"Błąd: {e}")

    # TAB 3 - ZMIANY (Filtrowanie i Wykres)
    with tab_rentownosc:
        st.header("Rentowność")
        c1, c2 = st.columns(2)
        d_r1 = c1.date_input("Od", date.today().replace(day=1), key='r1')
        d_r2 = c2.date_input("Do", date.today(), key='r2')
        
        st.divider()
        f_up = st.file_uploader("Plik 'analiza.xlsx'", type=['xlsx'])
        plik_do_uzycia = None
        
        if f_up:
            plik_do_uzycia = f_up
            if st.button("Zapisz w bazie"): zapisz_plik_w_bazie(conn, "analiza.xlsx", f_up.getvalue())
        else:
            b = wczytaj_plik_z_bazy(conn, "analiza.xlsx")
            if b: 
                plik_do_uzycia = io.BytesIO(b)
                st.info("Używam zapisanego pliku.")
                if st.button("Usuń plik"): usun_plik_z_bazy(conn, "analiza.xlsx")
        
        if st.button("Generuj Raport Rentowności", type="primary"):
            if not plik_do_uzycia: st.error("Brak pliku analizy.")
            else:
                # 1. BAZA (Paliwo/Opłaty)
                df_db = pobierz_dane_z_bazy(conn, d_r1, d_r2)
                df_db, _ = przygotuj_dane_paliwowe(df_db)
                
                koszty_agg = pd.DataFrame()
                if df_db is not None and not df_db.empty:
                    koszty_agg = df_db.groupby('identyfikator_clean').agg(
                        kb_netto=pd.NamedAgg('kwota_netto_eur', 'sum'),
                        kb_brutto=pd.NamedAgg('kwota_brutto_eur', 'sum')
                    )
                
                # 2. PLIK (Tylko Sprzedaż + Kontrahenci)
                df_analiza, df_analiza_raw = przetworz_plik_analizy(plik_do_uzycia, d_r1, d_r2)
                if df_analiza is None: df_analiza = pd.DataFrame(columns=['przychody_netto', 'przychody_brutto'])
                
                # 3. SUMA
                final = df_analiza.merge(koszty_agg, left_index=True, right_index=True, how='outer').fillna(0)
                final['ZYSK_NETTO'] = final['przychody_netto'] - final.get('kb_netto', 0)
                final['ZYSK_BRUTTO'] = final['przychody_brutto'] - final.get('kb_brutto', 0)
                
                st.session_state['res_rent'] = final
                st.session_state['raw_analiza'] = df_analiza_raw
                st.session_state['gotowe'] = True

        if st.session_state.get('gotowe'):
            res = st.session_state['res_rent']
            raw = st.session_state['raw_analiza']
            
            st.subheader("Podsumowanie Finansowe")
            c1, c2, c3 = st.columns(3)
            c1.metric("Przychody (Netto)", f"{res['przychody_netto'].sum():,.2f} EUR")
            c2.metric("Paliwo/Opłaty (Netto)", f"{res.get('kb_netto', pd.Series(0)).sum():,.2f} EUR")
            c3.metric("ZYSK (NETTO)", f"{res['ZYSK_NETTO'].sum():,.2f} EUR")
            
            st.divider()
            st.subheader("📊 Przychody wg Firm (Kontrahentów)")
            if raw is not None and not raw.empty:
                # Agregacja po kontrahencie
                chart_data = raw.groupby('kontrahent')['kwota_netto_eur'].sum().sort_values(ascending=False)
                st.bar_chart(chart_data)
                with st.expander("Dane wykresu"):
                    st.dataframe(chart_data.to_frame("Netto EUR"))
            else:
                st.info("Brak danych o przychodach do wykresu.")
                
            st.divider()
            st.subheader("Szczegóły per Pojazd")
            st.dataframe(res.sort_values('ZYSK_NETTO', ascending=False).style.format("{:,.2f} EUR"))

def check_pass():
    try: p = st.secrets["ADMIN_PASSWORD"]
    except: st.error("Brak hasła"); return False
    if st.session_state.get("ok"): return True
    if st.text_input("Hasło", type="password") == p and st.button("OK"):
        st.session_state["ok"] = True; st.rerun()
    return False

if __name__ == "__main__":
    if check_pass(): main_app()
