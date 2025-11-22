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
# Zmodyfikowana lista - nas interesuje głównie Faktura VAT sprzedaży w tym wariancie
ETYKIETY_PRZYCHODOW = [
    'Faktura VAT sprzedaży'
]

# Pozostałe listy (mogą być używane w innych miejscach, ale w rentowności będą ignorowane)
ETYKIETY_KOSZTOW_INNYCH = [
    'Faktura VAT zakupu', 'Korekta faktury VAT sprzedaży', 'Przychód wewnętrzny',
    'Korekta faktury VAT zakupu', 'Art. biurowe',
    'Art. chemiczne', 'Art. spożywcze', 'Badanie lekarskie', 'Delegacja',
    'Giełda', 'Księgowość', 'Leasing', 'Mandaty', 'Obsługa prawna',
    'Ogłoszenie', 'Poczta Polska', 'Program', 'Prowizje',
    'Rozliczanie kierowców', 'Rozliczenie VAT EUR', 'Serwis', 'Szkolenia BHP',
    'Tachograf', 'USŁ. HOTELOWA', 'Usługi telekomunikacyjne', 'Wykup auta',
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

# --- KATEGORYZACJA TRANSAKCJI (BEZ ZMIAN) ---
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

# --- NORMALIZACJA DANYCH (BEZ ZMIAN) ---
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

def wczytaj_i_zunifikuj_pliki(przeslane_pliki):
    lista_df_zunifikowanych = []
    for plik in przeslane_pliki:
        nazwa_pliku_base = plik.name
        st.write(f" - Przetwarzam: {nazwa_pliku_base}")
        try:
            if nazwa_pliku_base.endswith('.csv'):
                pass
            
            elif nazwa_pliku_base.endswith(('.xls', '.xlsx')):
                xls = pd.ExcelFile(plik, engine='openpyxl')
                
                if 'Transactions' in xls.sheet_names:
                    df_e100 = pd.read_excel(xls, sheet_name='Transactions')
                    kolumny_e100 = df_e100.columns
                    
                    if 'Numer samochodu' in kolumny_e100 and 'Kwota' in kolumny_e100:
                        st.write("    -> Wykryto format E100 (Polski)")
                        lista_df_zunifikowanych.append(normalizuj_e100_PL(df_e100))
                    elif 'Car registration number' in kolumny_e100 and 'Sum' in kolumny_e100:
                        st.write("    -> Wykryto format E100 (Angielski)")
                        lista_df_zunifikowanych.append(normalizuj_e100_EN(df_e100))
                    else:
                        st.warning(f"Pominięto plik {nazwa_pliku_base}. Arkusz 'Transactions' nie ma poprawnych kolumn.")
                
                elif 'Sheet0' in xls.sheet_names or len(xls.sheet_names) > 0:
                    df_eurowag = pd.read_excel(xls, sheet_name=0)
                    kolumny_eurowag = df_eurowag.columns
                    if 'Data i godzina' in kolumny_eurowag and 'Posiadacz karty' in kolumny_eurowag:
                        st.write("    -> Wykryto format Eurowag (Nowy)")
                        lista_df_zunifikowanych.append(normalizuj_eurowag(df_eurowag))
                    elif 'Data i godzina' in kolumny_eurowag and 'Artykuł' in kolumny_eurowag:
                         st.write("    -> Wykryto format Eurowag (Starszy)")
                         if 'Posiadacz karty' not in df_eurowag.columns:
                             df_eurowag['Posiadacz karty'] = None
                         lista_df_zunifikowanych.append(normalizuj_eurowag(df_eurowag))
                    else:
                         st.warning(f"Pominięto plik {nazwa_pliku_base}. Nie rozpoznano formatu Eurowag.")
                
                else:
                    st.warning(f"Pominięto plik {nazwa_pliku_base}. Nie rozpoznano formatu.")
                    
        except Exception as e:
           st.error(f"BŁĄD wczytania pliku {nazwa_pliku_base}: {e}")
    
    if not lista_df_zunifikowanych:
        return None, "Nie udało się zunifikować żadnych danych."
        
    polaczone_df = pd.concat(lista_df_zunifikowanych, ignore_index=True)
    return polaczone_df, None

# --- BAZA DANYCH ---
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
        st.success(f"SUKCES: Tabela '{NAZWA_TABELI_PLIKOW}' została utworzona na nowo!")
    except Exception as e:
        st.error(f"BŁĄD przy tworzeniu tabeli: {e}")

def wyczysc_duplikaty(conn):
    st.write("Czyszczenie duplikatów...")
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
              AND a.produkt = b.produkt
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
        if not isinstance(file_bytes, bytes):
            if hasattr(file_bytes, 'getvalue'):
                file_bytes = file_bytes.getvalue()
        
        with conn.session as s:
            s.execute(text(f"DELETE FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} WHERE file_name = :name"), {"name": file_name})
            s.commit()
            s.execute(text(f"""
                INSERT INTO {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} (file_name, file_data)
                VALUES (:name, :data)
            """), {"name": file_name, "data": file_bytes})
            s.commit()
            
        st.success(f"Zapisano plik '{file_name}' w bazie!")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"BŁĄD ZAPISU: {e}")

def wczytaj_plik_z_bazy(conn, file_name):
    try:
        with conn.session as s:
            exists = s.execute(text(f"SELECT to_regclass('{NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW}')")).scalar()
            if not exists:
                return None

            result = s.execute(
                text(f"SELECT file_data FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} WHERE file_name = :name"),
                {"name": file_name}
            ).fetchone()
            
            if result:
                dane = result[0]
                if isinstance(dane, memoryview):
                    return dane.tobytes()
                return dane
                
            return None
            
    except Exception as e:
        st.error(f"BŁĄD ODCZYTU PLIKU Z BAZY: {e}")
        return None

def usun_plik_z_bazy(conn, file_name):
    try:
        with conn.session as s:
            s.execute(text(f"DELETE FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} WHERE file_name = :name"), {"name": file_name})
            s.commit()
        st.success(f"Plik '{file_name}' został usunięty z bazy.")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.error(f"Błąd podczas usuwania pliku z bazy: {e}")

# --- CZYSZCZENIE KLUCZA ---
def bezpieczne_czyszczenie_klucza(s_identyfikatorow):
    s_str = s_identyfikatorow.astype(str)
    
    def clean_key(key):
        if key == 'nan' or not key:
            return 'Brak Identyfikatora'
        
        key_nospace = key.upper().replace(" ", "").replace("-", "").strip().strip('"')
        
        FIRMY_DO_USUNIECIA = [
            'TRUCK24SP', 'TRUCK24', 'EDENRED', 'MARMAR', 'SANTANDER',
            'LEASING', 'PZU', 'WARTA', 'INTERCARS', 'EUROWAG', 'E100'
        ]
        
        for firma in FIRMY_DO_USUNIECIA:
            if firma in key_nospace:
                return 'Brak Identyfikatora'

        if key_nospace.startswith("PL") and len(key_nospace) > 7:
            key_nospace = key_nospace[2:]
        
        if not key_nospace:
             return 'Brak Identyfikatora'

        if key_nospace.startswith("("):
            return key 
            
        match = re.search(r'([A-Z0-9]{4,12})', key_nospace)
        
        if match:
            found = match.group(1)
            if found in FIRMY_DO_USUNIECIA:
                return 'Brak Identyfikatora'
            return found
            
        if any(char.isdigit() for char in key_nospace):
             return key_nospace
             
        return 'Brak Identyfikatora'
            
    return s_str.apply(clean_key)

def przygotuj_dane_paliwowe(dane_z_bazy):
    if dane_z_bazy.empty:
        return dane_z_bazy, None
    dane_z_bazy['data_transakcji_dt'] = pd.to_datetime(dane_z_bazy['data_transakcji'])
    dane_z_bazy['identyfikator_clean'] = bezpieczne_czyszczenie_klucza(dane_z_bazy['identyfikator'])
    kurs_eur = pobierz_kurs_eur_pln()
    if not kurs_eur: return None, None
    unikalne_waluty = dane_z_bazy['waluta'].unique()
    mapa_kursow = pobierz_wszystkie_kursy(unikalne_waluty, kurs_eur)
    
    dane_z_bazy['kwota_netto_num'] = pd.to_numeric(dane_z_bazy['kwota_netto'], errors='coerce').fillna(0.0)
    dane_z_bazy['kwota_brutto_num'] = pd.to_numeric(dane_z_bazy['kwota_brutto'], errors='coerce').fillna(0.0)
    
    dane_z_bazy['kwota_netto_eur'] = dane_z_bazy.apply(
        lambda row: row['kwota_netto_num'] * mapa_kursow.get(row['waluta'], 0.0), axis=1
    )
    dane_z_bazy['kwota_brutto_eur'] = dane_z_bazy.apply(
        lambda row: row['kwota_brutto_num'] * mapa_kursow.get(row['waluta'], 0.0), axis=1
    )
    
    dane_z_bazy['kwota_finalna_eur'] = dane_z_bazy['kwota_brutto_eur']
    
    return dane_z_bazy, mapa_kursow

# --- FUNKCJA PARSOWANIA 'analiza.xlsx' (ZMODYFIKOWANA) ---
@st.cache_data
def przetworz_plik_analizy(przeslany_plik_bytes, data_start, data_stop):
    MAPA_WALUT_PLIKU = {
        'euro': 'EUR',
        'złoty polski': 'PLN',
        'korona duńska': 'DKK'
    }
    TYP_KWOTY_BRUTTO = 'Suma Wartosc_BruttoPoRabacie'
    TYP_KWOTY_NETTO = 'Suma Wartosc_NettoPoRabacie'
    
    try:
        kurs_eur_pln_nbp = pobierz_kurs_eur_pln()
        if not kurs_eur_pln_nbp:
            st.error("Nie udało się pobrać kursu EUR/PLN z NBP.")
            return None, None
        
        st.info(f"ℹ️ Przeliczam waluty po bieżącym kursie średnim NBP: 1 EUR = {kurs_eur_pln_nbp:.4f} PLN")
        
        lista_iso_walut = list(MAPA_WALUT_PLIKU.values())
        mapa_kursow = pobierz_wszystkie_kursy(lista_iso_walut, kurs_eur_pln_nbp)
    except Exception as e:
        st.error(f"Błąd podczas pobierania kursów walut NBP: {e}")
        return None, None
    
    try:
        df = pd.read_excel(przeslany_plik_bytes,
                           sheet_name='pojazdy',
                           engine='openpyxl',
                           header=[7, 8])
        
        kolumna_etykiet_tuple = df.columns[0]
        
        MAPA_BRUTTO_DO_KURSU = {}
        MAPA_NETTO_DO_KURSU = {}
        
        for col_waluta, col_typ in df.columns:
            if col_waluta in MAPA_WALUT_PLIKU and col_typ == TYP_KWOTY_BRUTTO:
                iso_code = MAPA_WALUT_PLIKU[col_waluta]
                kurs = mapa_kursow.get(iso_code, 0.0)
                if iso_code == 'EUR': kurs = 1.0
                MAPA_BRUTTO_DO_KURSU[(col_waluta, col_typ)] = kurs
            
            if col_waluta in MAPA_WALUT_PLIKU and col_typ == TYP_KWOTY_NETTO:
                iso_code = MAPA_WALUT_PLIKU[col_waluta]
                kurs = mapa_kursow.get(iso_code, 0.0)
                if iso_code == 'EUR': kurs = 1.0
                MAPA_NETTO_DO_KURSU[(col_waluta, col_typ)] = kurs
        
    except Exception as e:
        st.error(f"Nie udało się wczytać pliku Excel. Błąd: {e}")
        return None, None

    wyniki = []
    lista_aktualnych_pojazdow = []
    aktualny_kontrahent = "Nieznany"
    ostatnia_etykieta_pojazdu = None
    aktualna_data = None
    date_regex = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    
    # Funkcja wewnętrzna do wykrywania pojazdów
    def is_vehicle_line(line):
        if not line or line == 'nan': return False
        line_clean = str(line).strip().upper()
        BLACKLIST = [
            'E100', 'EUROWAG', 'VISA', 'MASTER', 'MASTERCARD',
            'ORLEN', 'LOTOS', 'BP', 'SHELL', 'UTA', 'DKV',
            'PKO', 'SANTANDER', 'ING', 'ALIOR', 'MILLENIUM',
            'TRUCK24SP', 'EDENRED', 'INTERCARS', 'MARMAR',
            'LEASING', 'FINANCE', 'UBER', 'BOLT', 'FREE',
            'SERWIS', 'POLSKA', 'SPOLKA', 'GROUP', 'LOGISTICS',
            'TRANS', 'CONSULTING', 'SYSTEM', 'SOLUTIONS',
            'ZAMÓWIENIE OD KLIENTA', 'FAKTURA VAT'
        ]
        if line_clean in BLACKLIST: return False
        
        words = re.split(r'[\s+Ii]+', line_clean)
        if not words: return False
        
        has_vehicle_word = False
        for word in words:
            if not word: continue
            word = word.replace("-", "")
            is_blacklisted = False
            for bad_word in BLACKLIST:
                if bad_word in word:
                    is_blacklisted = True
                    break
            if is_blacklisted: continue
            
            if len(word) < 5: continue
            if re.match(r'^[A-Z0-9]+$', word):
                ma_litery = any(c.isalpha() for c in word)
                ma_cyfry = any(c.isdigit() for c in word)
                if ma_litery and ma_cyfry:
                    has_vehicle_word = True
                    break
                if word.isdigit() and len(word) >= 4:
                    has_vehicle_word = True
                    break
        return has_vehicle_word

    for index, row in df.iterrows():
        try:
            etykieta_wiersza = str(row[kolumna_etykiet_tuple]).strip()
            kwota_brutto_eur = 0.0
            kwota_netto_eur = 0.0
            
            for col_tuple, kurs in MAPA_BRUTTO_DO_KURSU.items():
                if pd.notna(row[col_tuple]):
                    kwota_val = pd.to_numeric(row[col_tuple], errors='coerce')
                    if pd.isna(kwota_val): kwota_val = 0.0
                    kwota_brutto_eur += kwota_val * kurs
            
            for col_tuple, kurs in MAPA_NETTO_DO_KURSU.items():
                 if pd.notna(row[col_tuple]):
                    kwota_val = pd.to_numeric(row[col_tuple], errors='coerce')
                    if pd.isna(kwota_val): kwota_val = 0.0
                    kwota_netto_eur += kwota_val * kurs
            
            kwota_laczna = kwota_brutto_eur if kwota_brutto_eur != 0 else kwota_netto_eur

        except Exception:
            continue

        # 1. DATA
        if isinstance(row[kolumna_etykiet_tuple], (pd.Timestamp, date)) or date_regex.match(etykieta_wiersza):
            if isinstance(row[kolumna_etykiet_tuple], (pd.Timestamp, date)):
                aktualna_data = row[kolumna_etykiet_tuple].date()
            else:
                try: aktualna_data = pd.to_datetime(etykieta_wiersza).date()
                except: pass
            lista_aktualnych_pojazdow = []
            aktualny_kontrahent = "Nieznany"
            ostatnia_etykieta_pojazdu = None
            continue

        # 2. ETYKIETA (TYLKO 'Faktura VAT sprzedaży')
        elif etykieta_wiersza in WSZYSTKIE_ZNANE_ETYKIETY:
            if etykieta_wiersza == 'Faktura VAT sprzedaży':  # --- STRICT FILTER ---
                ostatnia_etykieta_pojazdu = etykieta_wiersza
                if kwota_laczna != 0.0:
                    etykieta_do_uzycia = ostatnia_etykieta_pojazdu
                    kwota_netto_do_uzycia = kwota_netto_eur
                    kwota_brutto_do_uzycia = kwota_brutto_eur
                    ostatnia_etykieta_pojazdu = None
                else:
                    continue
            else:
                # Ignoruj wszystko inne (Faktura Zakupu, Korekty itp.)
                ostatnia_etykieta_pojazdu = None
                continue

        # 3. KWOTA POZOSTAŁA W WIERSZU (Dla poprzedniej etykiety)
        elif (etykieta_wiersza == 'nan' or not etykieta_wiersza) and kwota_laczna != 0.0:
            if ostatnia_etykieta_pojazdu == 'Faktura VAT sprzedaży': # --- STRICT FILTER ---
                etykieta_do_uzycia = ostatnia_etykieta_pojazdu
                kwota_netto_do_uzycia = kwota_netto_eur
                kwota_brutto_do_uzycia = kwota_brutto_eur
                ostatnia_etykieta_pojazdu = None
            else:
                continue

        # 4. KONTEKST (POJAZD LUB KONTRAHENT)
        elif etykieta_wiersza != 'nan' and etykieta_wiersza:
            if is_vehicle_line(etykieta_wiersza):
                lista_aktualnych_pojazdow = re.split(r'\s+i\s+|\s+I\s+|\s*\+\s*', etykieta_wiersza, flags=re.IGNORECASE)
                lista_aktualnych_pojazdow = [p.strip() for p in lista_aktualnych_pojazdow if p.strip()]
            else:
                # Jeśli to nie data, nie kwota, nie znana etykieta i nie pojazd -> to nazwa firmy
                candidate = etykieta_wiersza.strip('"')
                # Ignoruj "Zamówienie od klienta" jako nazwę firmy
                if "Zamówienie" not in candidate:
                     aktualny_kontrahent = candidate
            continue
        
        else:
            continue

        # --- ZAPIS ---
        if 'etykieta_do_uzycia' in locals() and etykieta_do_uzycia:
            if not aktualna_data: continue
            if not (data_start <= aktualna_data <= data_stop): continue
            
            # --- IGNOROWANIE NIEPRZYPISANYCH ---
            if not lista_aktualnych_pojazdow:
                continue # Jeśli brak pojazdu, pomiń wiersz całkowicie

            liczba_pojazdow = len(lista_aktualnych_pojazdow)
            podz_kwota_brutto = kwota_brutto_do_uzycia / liczba_pojazdow
            podz_kwota_netto = kwota_netto_do_uzycia / liczba_pojazdow
            
            opis_transakcji = etykieta_do_uzycia
            
            for pojazd in lista_aktualnych_pojazdow:
                # Zawsze dodajemy jako przychód, bo filtrowaliśmy tylko Fakturę Sprzedaży
                wyniki.append({
                    'data': aktualna_data,
                    'pojazd_oryg': pojazd,
                    'opis': opis_transakcji,
                    'kontrahent': aktualny_kontrahent, # Dodano pole kontrahenta
                    'typ': 'Przychód (Subiekt)',
                    'zrodlo': 'Subiekt',
                    'kwota_brutto_eur': podz_kwota_brutto,
                    'kwota_netto_eur': podz_kwota_netto
                })
            
            del etykieta_do_uzycia
            kwota_brutto_do_uzycia = 0.0
            kwota_netto_do_uzycia = 0.0

    if not wyniki:
        st.warning(f"Nie znaleziono żadnych Faktur Sprzedaży dla aut w wybranym okresie ({data_start} - {data_stop}).")
        return None, None

    df_wyniki = pd.DataFrame(wyniki)
    
    CZARNA_LISTA_FINALNA = ['TRUCK24SP', 'EDENRED', 'MARMAR', 'INTERCARS', 'SANTANDER', 'LEASING']
    for smiec in CZARNA_LISTA_FINALNA:
        maska = df_wyniki['pojazd_oryg'].astype(str).str.upper().str.contains(smiec, na=False)
        df_wyniki = df_wyniki[~maska] # Usuwamy całkowicie rekordy "śmieciowe"

    df_wyniki['pojazd_clean'] = bezpieczne_czyszczenie_klucza(df_wyniki['pojazd_oryg'])
    
    # Usuwamy te, które po czyszczeniu wyszły jako 'Brak Identyfikatora'
    df_wyniki = df_wyniki[df_wyniki['pojazd_clean'] != 'Brak Identyfikatora']

    # GRUPOWANIE
    df_przychody = df_wyniki[df_wyniki['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kwota_brutto_eur'].sum().to_frame('przychody_brutto')
    df_przychody_netto = df_wyniki[df_wyniki['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kwota_netto_eur'].sum().to_frame('przychody_netto')
    
    # Koszty z pliku excel są teraz zawsze 0, bo je ignorujemy
    df_koszty = pd.DataFrame(0.0, index=df_przychody.index, columns=['koszty_inne_brutto'])
    df_koszty_netto = pd.DataFrame(0.0, index=df_przychody.index, columns=['koszty_inne_netto'])

    df_agregacja = pd.concat([df_przychody, df_przychody_netto, df_koszty, df_koszty_netto], axis=1).fillna(0)
    
    st.success(f"Przetworzono plik. Znaleziono {len(df_wyniki)} faktur sprzedaży dla aut.")
    return df_agregacja, df_wyniki

# --- APLIKACJA GŁÓWNA ---
def main_app():
    st.title("Analizator Wydatków Floty")

    @st.cache_data
    def to_excel(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=True, sheet_name='Raport')
        return output.getvalue()
    
    tab_admin, tab_raport, tab_rentownosc = st.tabs([
        "⚙️ Panel Admina",
        "📊 Raport Paliw/Opłat",
        "💰 Rentowność (Zysk/Strata)"
    ])

    try:
        conn = st.connection(NAZWA_POLACZENIA_DB, type="sql")
    except Exception as e:
        st.error(f"Nie udało się połączyć z bazą danych '{NAZWA_POLACZENIA_DB}'. Sprawdź 'Secrets'.")
        st.stop()

    # --- TAB 1: ADMIN ---
    with tab_admin:
        st.header("Panel Administracyjny")
        col1_admin, col2_admin = st.columns(2)
        with col1_admin:
            if st.button("1. Stwórz tabelę 'transactions'"):
                setup_database(conn)
                st.success("Gotowe.")
        with col2_admin:
             if st.button("2. Stwórz tabelę 'saved_files'"):
                setup_file_database(conn)
                st.success("Gotowe.")
        
        st.divider()
        st.subheader("Wgrywanie plików paliwowych (E100/Eurowag)")
        przeslane_pliki = st.file_uploader("Wybierz pliki", accept_multiple_files=True, type=['xlsx', 'xls'])
        if przeslane_pliki and st.button("Przetwórz i wgraj"):
             dane, blad = wczytaj_i_zunifikuj_pliki(przeslane_pliki)
             if dane is not None:
                 try:
                     dane.to_sql(NAZWA_TABELI, conn.engine, if_exists='append', index=False, schema=NAZWA_SCHEMATU)
                     wyczysc_duplikaty(conn)
                     st.success("Dane wgrane pomyślnie.")
                 except Exception as e:
                     st.error(f"Błąd SQL: {e}")
             else:
                 st.error(blad)

    # --- TAB 2: RAPORT PALIW ---
    with tab_raport:
        st.header("Raport Paliw i Opłat")
        try:
            min_max = conn.query(f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}")
            if min_max.empty or min_max.iloc[0,0] is None:
                st.info("Brak danych w bazie.")
            else:
                d_start = st.date_input("Start", value=min_max.iloc[0,0], key='r_start')
                d_stop = st.date_input("Stop", value=min_max.iloc[0,1], key='r_stop')
                
                df_db = pobierz_dane_z_bazy(conn, d_start, d_stop)
                df_db, _ = przygotuj_dane_paliwowe(df_db)
                
                if df_db is not None:
                    st.dataframe(df_db.head())
        except Exception as e:
            st.error(f"Błąd: {e}")

    # --- TAB 3: RENTOWNOŚĆ ---
    with tab_rentownosc:
        st.header("Raport Rentowności")
        
        # Daty
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            d_rent_start = st.date_input("Data od", value=date.today().replace(day=1), key="rent_start")
        with col_d2:
            d_rent_stop = st.date_input("Data do", value=date.today(), key="rent_stop")

        # Plik Analizy
        st.divider()
        plik_analizy = None
        uploaded_analiza = st.file_uploader("Wgraj 'analiza.xlsx'", type=['xlsx'])
        
        if uploaded_analiza:
            plik_analizy = uploaded_analiza
            if st.button("Zapisz plik w bazie"):
                zapisz_plik_w_bazie(conn, "analiza.xlsx", uploaded_analiza.getvalue())
        else:
            saved_bytes = wczytaj_plik_z_bazy(conn, "analiza.xlsx")
            if saved_bytes:
                st.info("Korzystam z zapisanego pliku 'analiza.xlsx'")
                plik_analizy = io.BytesIO(saved_bytes)
                if st.button("Usuń plik z bazy"):
                    usun_plik_z_bazy(conn, "analiza.xlsx")
            else:
                st.warning("Brak pliku analiza.xlsx")

        if st.button("Generuj Raport", type="primary"):
            if not plik_analizy:
                st.error("Musisz wgrać plik analizy.")
            else:
                with st.spinner("Przetwarzanie..."):
                    # 1. Pobierz koszty z bazy (paliwo)
                    df_db = pobierz_dane_z_bazy(conn, d_rent_start, d_rent_stop)
                    df_db, _ = przygotuj_dane_paliwowe(df_db)
                    
                    df_koszty_baza = pd.DataFrame()
                    if df_db is not None and not df_db.empty:
                        df_koszty_baza = df_db.groupby('identyfikator_clean').agg(
                            koszty_baza_netto=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                            koszty_baza_brutto=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                        )

                    # 2. Pobierz przychody z Excela (tylko faktury sprzedaży aut)
                    df_analiza, df_raw_analiza = przetworz_plik_analizy(plik_analizy, d_rent_start, d_rent_stop)
                    
                    if df_analiza is None:
                        df_analiza = pd.DataFrame(columns=['przychody_brutto', 'przychody_netto'])

                    # 3. Złącz
                    df_final = df_analiza.merge(df_koszty_baza, left_index=True, right_index=True, how='outer').fillna(0)
                    
                    # 4. Policz Zysk
                    df_final['ZYSK_NETTO'] = df_final['przychody_netto'] - df_final['koszty_baza_netto']
                    df_final['ZYSK_BRUTTO'] = df_final['przychody_brutto'] - df_final['koszty_baza_brutto']
                    
                    st.session_state['df_rentownosc'] = df_final
                    st.session_state['df_raw_analiza'] = df_raw_analiza
                    st.session_state['raport_ready'] = True

        if st.session_state.get('raport_ready'):
            df_final = st.session_state['df_rentownosc']
            df_raw = st.session_state['df_raw_analiza']
            
            # --- METRYKI OGÓLNE ---
            st.subheader("Podsumowanie")
            c1, c2, c3 = st.columns(3)
            c1.metric("Suma Przychód (Netto)", f"{df_final['przychody_netto'].sum():,.2f} EUR")
            c2.metric("Suma Koszty Paliwa/Opłat (Netto)", f"{df_final['koszty_baza_netto'].sum():,.2f} EUR")
            zysk_total = df_final['ZYSK_NETTO'].sum()
            c3.metric("ZYSK CAŁKOWITY (NETTO)", f"{zysk_total:,.2f} EUR", delta_color="normal")

            # --- WYKRES PRZYCHODÓW WG KONTRAHENTÓW ---
            st.divider()
            st.subheader("📊 Przychody łączne wg Firm (Kontrahentów)")
            
            if df_raw is not None and not df_raw.empty:
                # Grupujemy po kontrahencie, sumujemy kwotę netto
                chart_data = df_raw.groupby('kontrahent')['kwota_netto_eur'].sum().sort_values(ascending=False)
                
                # Wyświetlamy wykres słupkowy
                st.bar_chart(chart_data)
                
                # Opcjonalnie tabela pod wykresem
                with st.expander("Pokaż dane wykresu w tabeli"):
                    st.dataframe(chart_data.to_frame("Suma Netto EUR").style.format("{:,.2f} EUR"), use_container_width=True)
            else:
                st.info("Brak danych do wygenerowania wykresu.")

            # --- TABELA SZCZEGÓŁOWA ---
            st.divider()
            st.subheader("Szczegóły per Pojazd")
            st.dataframe(
                df_final[['przychody_netto', 'koszty_baza_netto', 'ZYSK_NETTO']].sort_values(by='ZYSK_NETTO', ascending=False)
                .style.format("{:,.2f} EUR"),
                use_container_width=True
            )
            st.download_button("Pobierz Excel", data=to_excel(df_final), file_name="rentownosc.xlsx")

# --- LOGOWANIE ---
def check_password():
    try:
        prawidlowe_haslo = st.secrets["ADMIN_PASSWORD"]
    except:
        st.error("Brak hasła w st.secrets!")
        return False

    if st.session_state.get("password_correct", False):
        return True

    haslo = st.text_input("Hasło", type="password")
    if st.button("Zaloguj"):
        if haslo == prawidlowe_haslo:
            st.session_state["password_correct"] = True
            st.rerun()
        else:
            st.error("Błędne hasło")
    return False

if __name__ == "__main__":
    if check_password():
        main_app()
