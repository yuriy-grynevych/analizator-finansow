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
# POPRAWKA: Usunięto 'Korekta faktury VAT zakupu' z przychodów
ETYKIETY_PRZYCHODOW = [
    'Faktura VAT sprzedaży', 'Przychód wewnętrzny'
]

# ZMODYFIKOWANO: Lista kosztów (ignorowana w logice, ale tutaj definiowana)
# Dodano tu 'Korekta faktury VAT zakupu', żeby była traktowana jako koszt/inny
ETYKIETY_KOSZTOW_INNYCH = [
    'Faktura VAT zakupu', 'Korekta faktury VAT sprzedaży', 'Korekta faktury VAT zakupu', 
    'Art. biurowe', 'Art. chemiczne', 'Art. spożywcze', 'Badanie lekarskie', 'Delegacja', 
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
    
# --- NOWE FUNKCJE "TŁUMACZENIA" (BEZ ZMIAN) ---
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

# --- FUNKCJA DO WCZYTYWANIA PLIKÓW (BEZ ZMIAN) ---
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
                        st.write("   -> Wykryto format E100 (Polski)")
                        lista_df_zunifikowanych.append(normalizuj_e100_PL(df_e100))
                    elif 'Car registration number' in kolumny_e100 and 'Sum' in kolumny_e100:
                        st.write("   -> Wykryto format E100 (Angielski)")
                        lista_df_zunifikowanych.append(normalizuj_e100_EN(df_e100))
                    else:
                        st.warning(f"Pominięto plik {nazwa_pliku_base}. Arkusz 'Transactions' nie ma poprawnych kolumn.")
                
                elif 'Sheet0' in xls.sheet_names or len(xls.sheet_names) > 0:
                    df_eurowag = pd.read_excel(xls, sheet_name=0) 
                    kolumny_eurowag = df_eurowag.columns
                    if 'Data i godzina' in kolumny_eurowag and 'Posiadacz karty' in kolumny_eurowag:
                        st.write("   -> Wykryto format Eurowag (Nowy)")
                        lista_df_zunifikowanych.append(normalizuj_eurowag(df_eurowag))
                    elif 'Data i godzina' in kolumny_eurowag and 'Artykuł' in kolumny_eurowag:
                         st.write("   -> Wykryto format Eurowag (Starszy)")
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

# --- NAPRAWIONE FUNKCJE BAZY DANYCH (WERSJA DEBUG) ---
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
    """Niszczy starą tabelę i tworzy nową poprawną."""
    try:
        with conn.session as s:
            # Najpierw usuwamy, żeby nie było konfliktów struktur
            s.execute(text(f"DROP TABLE IF EXISTS {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW}"))
            s.commit()
            
            # Tworzymy nową
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
        # Konwersja do bytes, jeśli to buffer
        if not isinstance(file_bytes, bytes):
            if hasattr(file_bytes, 'getvalue'):
                file_bytes = file_bytes.getvalue()
        
        with conn.session as s:
            # Usuń stary wpis
            s.execute(text(f"DELETE FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} WHERE file_name = :name"), {"name": file_name})
            s.commit()
            
            # Wstaw nowy
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

# --- POPRAWIONA FUNKCJA ODCZYTU (BEZ DEKORATORA CACHE!) ---
def wczytaj_plik_z_bazy(conn, file_name):
    try:
        # Używamy sesji bezpośrednio, aby uniknąć problemów z DataFrame i Pandas
        with conn.session as s:
            # Najpierw sprawdzamy czy tabela istnieje (dla bezpieczeństwa)
            exists = s.execute(text(f"SELECT to_regclass('{NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW}')")).scalar()
            if not exists:
                return None

            # Pobieramy plik
            result = s.execute(
                text(f"SELECT file_data FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI_PLIKOW} WHERE file_name = :name"), 
                {"name": file_name}
            ).fetchone()
            
            if result:
                dane = result[0]
                # Konwersja memoryview na bytes (dla pewności)
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
# --- OSTATECZNA WERSJA CZYSZCZENIA (Z TWARDĄ BLOKADĄ FIRM) ---
def bezpieczne_czyszczenie_klucza(s_identyfikatorow):
    s_str = s_identyfikatorow.astype(str)
    
    def clean_key(key):
        if key == 'nan' or not key: 
            return 'Brak Identyfikatora'
        
        # 1. Usuwamy spacje, myślniki i cudzysłowy
        key_nospace = key.upper().replace(" ", "").replace("-", "").strip().strip('"')
        
        # --- BLOKADA FIRM (Hardcoded Blacklist) ---
        # Te słowa są usuwane BEZWARUNKOWO, nawet jak wyglądają jak rejestracja.
        FIRMY_DO_USUNIECIA = [
            'TRUCK24SP', 'TRUCK24', 'EDENRED', 'MARMAR', 'SANTANDER', 
            'LEASING', 'PZU', 'WARTA', 'INTERCARS', 'EUROWAG', 'E100'
        ]
        
        # Sprawdzamy czy klucz ZAWIERA którąś z zakazanych nazw
        for firma in FIRMY_DO_USUNIECIA:
            if firma in key_nospace:
                return 'Brak Identyfikatora'

        # --- LOGIKA "PL" (dla długich ciągów powyżej 7 znaków) ---
        if key_nospace.startswith("PL") and len(key_nospace) > 7:
            key_nospace = key_nospace[2:] # Ucinamy pierwsze dwa znaki (PL)
        
        if not key_nospace:
             return 'Brak Identyfikatora'

        if key_nospace.startswith("("):
            return key 
            
        # 2. Szukamy wzorca rejestracji
        match = re.search(r'([A-Z0-9]{4,12})', key_nospace)
        
        if match:
            found = match.group(1)
            # Ostatnie sito - czy to co znaleźliśmy nie jest na liście zakazanej?
            if found in FIRMY_DO_USUNIECIA:
                return 'Brak Identyfikatora'
            return found
            
        # Jeśli nic nie znalazł regexem, ale ma cyfry
        if any(char.isdigit() for char in key_nospace):
             return key_nospace
             
        return 'Brak Identyfikatora'
            
    return s_str.apply(clean_key)
# --- NOWA FUNKCJA PRZYGOTOWUJĄCA DANE PALIWOWE ---
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
    # --- 1. MAPOWANIE WALUT ---
    MAPA_WALUT_PLIKU = {
        'euro': 'EUR',
        'złoty polski': 'PLN',
        'korona duńska': 'DKK'
    }
    TYP_KWOTY_BRUTTO = 'Suma Wartosc_BruttoPoRabacie'
    TYP_KWOTY_NETTO = 'Suma Wartosc_NettoPoRabacie'
    
    # --- 2. POBIERANIE KURSÓW WALUT ---
    try:
        kurs_eur_pln_nbp = pobierz_kurs_eur_pln()
        if not kurs_eur_pln_nbp:
            st.error("Nie udało się pobrać kursu EUR/PLN z NBP. Przetwarzanie przerwane.")
            return None, None
        
        # INFO O KURSIE DLA UŻYTKOWNIKA
        st.info(f"ℹ️ Przeliczam waluty po bieżącym kursie średnim NBP: 1 EUR = {kurs_eur_pln_nbp:.4f} PLN")
        
        lista_iso_walut = list(MAPA_WALUT_PLIKU.values())
        mapa_kursow = pobierz_wszystkie_kursy(lista_iso_walut, kurs_eur_pln_nbp)
    except Exception as e:
        st.error(f"Błąd podczas pobierania kursów walut NBP: {e}")
        return None, None
    
    # --- 3. WCZYTANIE PLIKU ---
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

    # --- 4. LOGIKA PARSOWANIA ---
    wyniki = []
    lista_aktualnych_pojazdow = [] 
    aktualny_kontrahent = None 
    ostatnia_etykieta_pojazdu = None
    aktualna_data = None            
    date_regex = re.compile(r'^\d{4}-\d{2}-\d{2}$') 
    
    # --- FUNKCJA WEWNĘTRZNA: CZY TO POJAZD? ---
    def is_vehicle_line(line):
        if not line or line == 'nan':
            return False
        
        line_clean = str(line).strip().upper()
        
        # CZARNA LISTA FIRM I SŁÓW KLUCZOWYCH
        BLACKLIST = [
            'E100', 'EUROWAG', 'VISA', 'MASTER', 'MASTERCARD', 
            'ORLEN', 'LOTOS', 'BP', 'SHELL', 'UTA', 'DKV', 
            'PKO', 'SANTANDER', 'ING', 'ALIOR', 'MILLENIUM',
            'TRUCK24SP', 'EDENRED', 'INTERCARS', 'MARMAR',
            'LEASING', 'FINANCE', 'UBER', 'BOLT', 'FREE',
            'SERWIS', 'POLSKA', 'SPOLKA', 'GROUP', 'LOGISTICS',
            'TRANS', 'CONSULTING', 'SYSTEM', 'SOLUTIONS'
        ]
        
        if line_clean in BLACKLIST:
            return False

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
            if is_blacklisted:
                continue
            
            if len(word) < 5:
                continue
            
            if re.match(r'^[A-Z0-9]+$', word):
                ma_litery = any(c.isalpha() for c in word)
                ma_cyfry = any(c.isdigit() for c in word)
                
                if ma_litery and ma_cyfry:
                    has_vehicle_word = True
                    break
                
                if word.isdigit() and len(word) >= 4:
                    has_vehicle_word = True
                    break

        if not has_vehicle_word:
            return False
            
        for word in words:
            if len(word) > 12: 
                return False
                
        return True

    # --- GŁÓWNA PĘTLA ---
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

        except Exception as e_row:
            continue # Pomijamy wiersze z błędami odczytu

        # BLOK 1: Szukanie daty
        if isinstance(row[kolumna_etykiet_tuple], (pd.Timestamp, date)) or date_regex.match(etykieta_wiersza):
            if isinstance(row[kolumna_etykiet_tuple], (pd.Timestamp, date)):
                aktualna_data = row[kolumna_etykiet_tuple].date()
            else:
                try: aktualna_data = pd.to_datetime(etykieta_wiersza).date()
                except: pass
            lista_aktualnych_pojazdow = [] 
            aktualny_kontrahent = None
            ostatnia_etykieta_pojazdu = None
            continue

        # BLOK 2: Wiersz z etykietą
        elif etykieta_wiersza in WSZYSTKIE_ZNANE_ETYKIETY:
            if etykieta_wiersza not in ETYKIETY_IGNOROWANE:
                ostatnia_etykieta_pojazdu = etykieta_wiersza
                if kwota_laczna != 0.0:
                    etykieta_do_uzycia = ostatnia_etykieta_pojazdu
                    kwota_netto_do_uzycia = kwota_netto_eur
                    kwota_brutto_do_uzycia = kwota_brutto_eur
                    ostatnia_etykieta_pojazdu = None 
                else:
                    continue
            else:
                continue 

        # BLOK 3: Wiersz z kwotą
        elif (etykieta_wiersza == 'nan' or not etykieta_wiersza) and kwota_laczna != 0.0:
            if ostatnia_etykieta_pojazdu: 
                etykieta_do_uzycia = ostatnia_etykieta_pojazdu
                kwota_netto_do_uzycia = kwota_netto_eur
                kwota_brutto_do_uzycia = kwota_brutto_eur
                ostatnia_etykieta_pojazdu = None 
            else:
                continue 

        # BLOK 4: Wiersz kontekstowy
        elif etykieta_wiersza != 'nan' and etykieta_wiersza:
            if is_vehicle_line(etykieta_wiersza):
                lista_aktualnych_pojazdow = re.split(r'\s+i\s+|\s+I\s+|\s*\+\s*', etykieta_wiersza, flags=re.IGNORECASE)
                lista_aktualnych_pojazdow = [p.strip() for p in lista_aktualnych_pojazdow if p.strip()]
            else:
                aktualny_kontrahent = etykieta_wiersza.strip('"')
            continue
        
        else:
            continue 

        # --- BLOK 5: ZAPISYWANIE WYNIKÓW ---
        if 'etykieta_do_uzycia' in locals() and etykieta_do_uzycia:
            
            if not aktualna_data: continue 
            if not (data_start <= aktualna_data <= data_stop): continue 

            pojazdy_do_zapisu = []
            
            # --- ZMIANA: LOGIKA PRZYPISYWANIA ---
            # 1. Jeśli wykryto pojazdy (np. w tytule "Auto DW1234") -> Używamy ich
            if lista_aktualnych_pojazdow:
                pojazdy_do_zapisu = lista_aktualnych_pojazdow
            
            # 2. Jeśli NIE ma pojazdu, ale to PRZYCHÓD i mamy KONTRAHENTA 
            # -> Przypisujemy do Kontrahenta (traktując firmę jako źródło przychodu)
            elif etykieta_do_uzycia in ETYKIETY_PRZYCHODOW and aktualny_kontrahent and aktualny_kontrahent != "nan":
                pojazdy_do_zapisu = [aktualny_kontrahent]
            
            # 3. W przeciwnym razie (koszty bez auta, inne bez auta) -> POMIŃ
            else:
                continue
            
            liczba_pojazdow = len(pojazdy_do_zapisu)
            podz_kwota_brutto = kwota_brutto_do_uzycia / liczba_pojazdow
            podz_kwota_netto = kwota_netto_do_uzycia / liczba_pojazdow
            
            opis_transakcji = etykieta_do_uzycia
            kontrahent_do_zapisu = "Brak Kontrahenta"
            if aktualny_kontrahent and aktualny_kontrahent != "nan":
                opis_transakcji = f"{etykieta_do_uzycia} - {aktualny_kontrahent}"
                kontrahent_do_zapisu = aktualny_kontrahent
            
            for pojazd in pojazdy_do_zapisu:
                # --- TYLKO PRZYCHODY ---
                if etykieta_do_uzycia in ETYKIETY_PRZYCHODOW:
                    wyniki.append({
                        'data': aktualna_data, 'pojazd_oryg': pojazd, 'opis': opis_transakcji,
                        'typ': 'Przychód (Subiekt)', 'zrodlo': 'Subiekt',
                        'kwota_brutto_eur': podz_kwota_brutto,
                        'kwota_netto_eur': podz_kwota_netto,
                        'kontrahent': kontrahent_do_zapisu # DODANE DO WYKRESU
                    })
                # USUNIĘTO ELIF DLA KOSZTÓW INNYCH (IGNOROWANIE WYDATKÓW)
            
            del etykieta_do_uzycia
            kwota_brutto_do_uzycia = 0.0
            kwota_netto_do_uzycia = 0.0
            
    # --- AGREGACJA I CZYSZCZENIE FINALNE ---
    if not wyniki:
        st.warning(f"Nie znaleziono żadnych PRZYCHODÓW w pliku dla wybranego okresu ({data_start} - {data_stop}).")
        return None, None 

    df_wyniki = pd.DataFrame(wyniki)
    
    # BRUTALNY FILTR - OSTATNIA DESKA RATUNKU
    CZARNA_LISTA_FINALNA = ['TRUCK24SP', 'EDENRED', 'MARMAR', 'INTERCARS', 'SANTANDER', 'LEASING']
    for smiec in CZARNA_LISTA_FINALNA:
        maska = df_wyniki['pojazd_oryg'].astype(str).str.upper().str.contains(smiec, na=False)
        # Tu też zmieniamy - jeśli śmieć, to usuwamy wiersz, bo nie chcemy 'Ogólne'
        df_wyniki = df_wyniki[~maska]

    if df_wyniki.empty:
         st.warning("Wszystkie znalezione transakcje zostały odfiltrowane (brak poprawnego pojazdu).")
         return None, None

    df_wyniki['pojazd_clean'] = bezpieczne_czyszczenie_klucza(df_wyniki['pojazd_oryg'])
    
    # Usuwamy te, które po czyszczeniu stały się "Brak Identyfikatora"
    # df_wyniki = df_wyniki[df_wyniki['pojazd_clean'] != 'Brak Identyfikatora']
    # ZMIANA: Zostawiamy "Brak Identyfikatora" jeśli jest to przychód, bo może to być po prostu Kontrahent
    # (bezpieczne_czyszczenie_klucza zamienia np. "KUEHNE+NAGEL" na "Brak Identyfikatora" przez blacklistę firm)
    # Musimy przywrócić oryginalną nazwę jeśli clean zwrócił 'Brak', a to jest przychód od firmy
    
    maska_brak = df_wyniki['pojazd_clean'] == 'Brak Identyfikatora'
    df_wyniki.loc[maska_brak, 'pojazd_clean'] = df_wyniki.loc[maska_brak, 'pojazd_oryg']
    
    
    # GRUPOWANIE
    df_przychody = df_wyniki[df_wyniki['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kwota_brutto_eur'].sum().to_frame('przychody_brutto')
    df_przychody_netto = df_wyniki[df_wyniki['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kwota_netto_eur'].sum().to_frame('przychody_netto')
    
    # Puste ramki dla kosztów (bo je ignorujemy z pliku)
    df_koszty = pd.DataFrame(columns=['koszty_inne_brutto'])
    df_koszty_netto = pd.DataFrame(columns=['koszty_inne_netto'])

    df_agregacja = pd.concat([df_przychody, df_przychody_netto, df_koszty, df_koszty_netto], axis=1).fillna(0)
    
    st.success(f"Plik analizy przetworzony pomyślnie. Znaleziono {len(df_wyniki)} wpisów przychodowych.")
    return df_agregacja, df_wyniki

# --- FUNKCJA main() (ZE ZMIANAMI) ---
def main_app():
    
    st.title("Analizator Wydatków Floty") 

    @st.cache_data
    def to_excel(df):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=True, sheet_name='Raport')
        processed_data = output.getvalue()
        return processed_data
    
    tab_admin, tab_raport, tab_rentownosc = st.tabs([
        "⚙️ Panel Admina",
        "📊 Raport Paliw/Opłat", 
        "💰 Rentowność (Zysk/Strata)"
    ])

    try:
        conn = st.connection(NAZWA_POLACZENIA_DB, type="sql")
    except Exception as e:
        st.error(f"Nie udało się połączyć z bazą danych '{NAZWA_POLACZENIA_DB}'. Sprawdź 'Secrets' w Ustawieniach.")
        st.stop() 

    # --- ZAKŁADKA 1: PANEL ADMINA ---
    with tab_admin:
        st.header("Panel Administracyjny")
        st.success("Zalogowano pomyślnie!")

        col1_admin, col2_admin = st.columns(2)
        with col1_admin:
            st.subheader("Baza Danych Transakcji")
            if st.button("1. Stwórz tabelę 'transactions' (tylko raz!)"):
                with st.spinner("Tworzenie tabeli..."):
                    setup_database(conn)
                st.success("Tabela 'transactions' jest gotowa.")
        
        with col2_admin:
            st.subheader("Baza Danych Plików")
            if st.button("2. Stwórz tabelę 'saved_files' (tylko raz!)"):
                with st.spinner("Tworzenie tabeli..."):
                    setup_file_database(conn)

        st.divider()
        st.subheader("Wgrywanie nowych plików (Paliwo/Opłaty)")
        przeslane_pliki = st.file_uploader(
            "Wybierz pliki Eurowag i E100 do dodania do bazy",
            accept_multiple_files=True,
            type=['xlsx', 'xls']
        )
        
        if przeslane_pliki:
            if st.button("Przetwórz i wgraj pliki do bazy", type="primary"):
                with st.spinner("Wczytywanie i unifikowanie plików..."):
                    dane_do_wgrania, blad = wczytaj_i_zunifikuj_pliki(przeslane_pliki)
                
                if blad:
                    st.error(blad)
                elif dane_do_wgrania is None or dane_do_wgrania.empty:
                    st.error("Nie udało się przetworzyć żadnych danych. Sprawdź pliki.")
                else:
                    st.success(f"Zunifikowano {len(dane_do_wgrania)} nowych transakcji.")
                    
                    with st.spinner("Zapisywanie danych w bazie..."):
                        try:
                            dane_do_wgrania.to_sql(
                                NAZWA_TABELI, 
                                conn.engine, 
                                if_exists='append', 
                                index=False, 
                                schema=NAZWA_SCHEMATU
                            )
                        except Exception as e:
                            st.error(f"Błąd podczas zapisu do bazy: {e}")
                            st.info("WSKAZÓWKA: Czy na pewno kliknąłeś 'Stwórz tabelę w bazie danych'?")
                            st.stop()
                            
                    st.success("Dane zostały pomyślnie zapisane w bazie!")
                    
                    with st.spinner("Czyszczenie duplikatów..."):
                        wyczysc_duplikaty(conn)
                    st.success("Baza danych została oczyszczona. Gotowe!")
                    st.info("Teraz możesz przejść do zakładki 'Raport Paliw/Opłat'.")

    # --- ZAKŁADKA 2: RAPORT GŁÓWNY (PRZEBUDOWANA) ---
    with tab_raport:
        st.header("Szczegółowy Raport Paliw i Opłat")
        
        try:
            min_max_date_query = f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}"
            min_max_date = conn.query(min_max_date_query)
            
            if min_max_date.empty or min_max_date.iloc[0, 0] is None:
                st.info("Baza danych jest pusta. Przejdź do Panelu Admina, aby wgrać pliki.")
            else:
                domyslny_start = min_max_date.iloc[0, 0]
                domyslny_stop = min_max_date.iloc[0, 1]

                col1, col2 = st.columns(2)
                with col1:
                    data_start_rap = st.date_input("Data Start", value=domyslny_start, min_value=domyslny_start, max_value=domyslny_stop, key="rap_start")
                with col2:
                    data_stop_rap = st.date_input("Data Stop", value=domyslny_stop, min_value=domyslny_start, max_value=domyslny_stop, key="rap_stop")

                dane_z_bazy_full = pobierz_dane_z_bazy(conn, data_start_rap, data_stop_rap)
                
                if dane_z_bazy_full.empty:
                    st.warning(f"Brak danych paliwowych w wybranym zakresie dat ({data_start_rap} - {data_stop_rap}).")
                else:
                    dane_przygotowane, mapa_kursow = przygotuj_dane_paliwowe(dane_z_bazy_full.copy())
                    
                    if dane_przygotowane is None: st.stop()
                    
                    sub_tab_paliwo, sub_tab_oplaty, sub_tab_inne = st.tabs(["⛽ Paliwo", "🛣️ Opłaty Drogowe", "🛒 Pozostałe"])
                    
                    with sub_tab_paliwo:
                        df_paliwo = dane_przygotowane[dane_przygotowane['typ'] == 'PALIWO']
                        if df_paliwo.empty:
                            st.info("Brak danych o paliwie w tym okresie.")
                        else:
                            st.subheader("Wydatki na Paliwo (Diesel + AdBlue)")
                            filtr_paliwo = st.text_input("Filtruj pojazd:", key="filtr_paliwo").upper()
                            podsumowanie_paliwo_kwoty = df_paliwo.groupby('identyfikator_clean').agg(
                                Kwota_Netto_EUR=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                                Kwota_Brutto_EUR=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                            )
                            podsumowanie_litry = df_paliwo.groupby(['identyfikator_clean', 'produkt'])['ilosc'].sum().unstack(fill_value=0)
                            if 'Diesel' not in podsumowanie_litry.columns: podsumowanie_litry['Diesel'] = 0
                            if 'AdBlue' not in podsumowanie_litry.columns: podsumowanie_litry['AdBlue'] = 0
                            podsumowanie_litry = podsumowanie_litry.rename(columns={'Diesel': 'Litry (Diesel)', 'AdBlue': 'Litry (AdBlue)'})
                            podsumowanie_paliwo = podsumowanie_paliwo_kwoty.merge(podsumowanie_litry, left_index=True, right_index=True, how='left').fillna(0)
                            podsumowanie_paliwo = podsumowanie_paliwo.sort_values(by='Kwota_Brutto_EUR', ascending=False)
                            if filtr_paliwo:
                                podsumowanie_paliwo = podsumowanie_paliwo[podsumowanie_paliwo.index.str.contains(filtr_paliwo, na=False)]
                            st.metric(label="Suma Łączna (Paliwo)", value=f"{podsumowanie_paliwo['Kwota_Brutto_EUR'].sum():,.2f} EUR")
                            columny_do_pokazania = ['Kwota_Netto_EUR', 'Kwota_Brutto_EUR', 'Litry (Diesel)', 'Litry (AdBlue)']
                            formatowanie = {'Kwota_Netto_EUR': '{:,.2f} EUR', 'Kwota_Brutto_EUR': '{:,.2f} EUR', 'Litry (Diesel)': '{:,.2f} L', 'Litry (AdBlue)': '{:,.2f} L'}
                            st.dataframe(podsumowanie_paliwo[columny_do_pokazania].style.format(formatowanie), use_container_width=True)
                            st.download_button(label="Pobierz raport jako Excel (.xlsx)", data=to_excel(podsumowanie_paliwo), file_name=f"raport_paliwo_{data_start_rap}_do_{data_stop_rap}.xlsx", mime="application/vnd.ms-excel")
                            st.divider()
                            st.subheader("Szczegóły transakcji paliwowych")
                            lista_pojazdow_paliwo = ["--- Wybierz pojazd ---"] + list(podsumowanie_paliwo.index)
                            wybrany_pojazd_paliwo = st.selectbox("Wybierz identyfikator:", lista_pojazdow_paliwo)
                            if wybrany_pojazd_paliwo != "--- Wybierz pojazd ---":
                                df_szczegoly = df_paliwo[df_paliwo['identyfikator_clean'] == wybrany_pojazd_paliwo].sort_values(by='data_transakcji_dt', ascending=False)
                                df_szczegoly_display = df_szczegoly[['data_transakcji_dt', 'produkt', 'ilosc', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                st.dataframe(
                                    df_szczegoly_display.rename(columns={'data_transakcji_dt': 'Data', 'produkt': 'Produkt', 'ilosc': 'Litry', 'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'}),
                                    use_container_width=True, hide_index=True,
                                    column_config={"Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"), "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"), "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"), "Litry": st.column_config.NumberColumn(format="%.2f L"),}
                                )
                    with sub_tab_oplaty:
                        df_oplaty = dane_przygotowane[dane_przygotowane['typ'] == 'OPŁATA']
                        if df_oplaty.empty:
                            st.info("Brak danych o opłatach drogowych w tym okresie.")
                        else:
                            st.subheader("Wydatki na Opłaty Drogowe")
                            filtr_oplaty = st.text_input("Filtruj pojazd:", key="filtr_oplaty").upper()
                            podsumowanie_oplaty = df_oplaty.groupby('identyfikator_clean').agg(
                                Kwota_Netto_EUR=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                                Kwota_Brutto_EUR=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                            ).sort_values(by='Kwota_Brutto_EUR', ascending=False)
                            if filtr_oplaty:
                                podsumowanie_oplaty = podsumowanie_oplaty[podsumowanie_oplaty.index.str.contains(filtr_oplaty, na=False)]
                            st.metric(label="Suma Łączna (Opłaty Drogowe)", value=f"{podsumowanie_oplaty['Kwota_Brutto_EUR'].sum():,.2f} EUR")
                            st.dataframe(podsumowanie_oplaty.style.format({'Kwota_Netto_EUR': '{:,.2f} EUR', 'Kwota_Brutto_EUR': '{:,.2f} EUR'}), use_container_width=True)
                            st.download_button(label="Pobierz raport jako Excel (.xlsx)", data=to_excel(podsumowanie_oplaty), file_name=f"raport_oplaty_{data_start_rap}_do_{data_stop_rap}.xlsx", mime="application/vnd.ms-excel")
                            st.divider()
                            st.subheader("Szczegóły transakcji (Opłaty)")
                            lista_pojazdow_oplaty = ["--- Wybierz pojazd ---"] + list(podsumowanie_oplaty.index)
                            wybrany_pojazd_oplaty = st.selectbox("Wybierz identyfikator:", lista_pojazdow_oplaty, key="select_oplaty")
                            if wybrany_pojazd_oplaty != "--- Wybierz pojazd ---":
                                df_szczegoly_oplaty = df_oplaty[df_oplaty['identyfikator_clean'] == wybrany_pojazd_oplaty].sort_values(by='data_transakcji_dt', ascending=False)
                                df_szczegoly_oplaty_display = df_szczegoly_oplaty[['data_transakcji_dt', 'produkt', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                st.dataframe(
                                    df_szczegoly_oplaty_display.rename(columns={'data_transakcji_dt': 'Data', 'produkt': 'Opis', 'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'}),
                                    use_container_width=True, hide_index=True,
                                    column_config={"Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"), "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"), "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),}
                                )
                    with sub_tab_inne:
                        df_inne = dane_przygotowane[dane_przygotowane['typ'] == 'INNE']
                        if df_inne.empty:
                            st.info("Brak danych o pozostałych wydatkach w tym okresie.")
                        else:
                            st.subheader("Pozostałe Wydatki (np. Płatności kartą)")
                            filtr_inne = st.text_input("Filtruj pojazd:", key="filtr_inne").upper()
                            podsumowanie_inne = df_inne.groupby('identyfikator_clean').agg(
                                Kwota_Netto_EUR=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                                Kwota_Brutto_EUR=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                            ).sort_values(by='Kwota_Brutto_EUR', ascending=False)
                            if filtr_inne:
                                podsumowanie_inne = podsumowanie_inne[podsumowanie_inne.index.str.contains(filtr_inne, na=False)]
                            st.metric(label="Suma Łączna (Pozostałe)", value=f"{podsumowanie_inne['Kwota_Brutto_EUR'].sum():,.2f} EUR")
                            st.dataframe(podsumowanie_inne.style.format({'Kwota_Netto_EUR': '{:,.2f} EUR', 'Kwota_Brutto_EUR': '{:,.2f} EUR'}), use_container_width=True)
                            st.download_button(label="Pobierz raport jako Excel (.xlsx)", data=to_excel(podsumowanie_inne), file_name=f"raport_inne_{data_start_rap}_do_{data_stop_rap}.xlsx", mime="application/vnd.ms-excel")
                            st.divider()
                            st.subheader("Szczegóły transakcji (Inne)")
                            lista_pojazdow_inne = ["--- Wybierz pojazd ---"] + list(podsumowanie_inne.index)
                            wybrany_pojazd_inne = st.selectbox("Wybierz identyfikator:", lista_pojazdow_inne, key="select_inne")
                            if wybrany_pojazd_inne != "--- Wybierz pojazd ---":
                                df_szczegoly_inne = df_inne[df_inne['identyfikator_clean'] == wybrany_pojazd_inne].sort_values(by='data_transakcji_dt', ascending=False)
                                df_szczegoly_inne_display = df_szczegoly_inne[['data_transakcji_dt', 'produkt', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                st.dataframe(
                                    df_szczegoly_inne_display.rename(columns={'data_transakcji_dt': 'Data', 'produkt': 'Opis', 'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'}),
                                    use_container_width=True, hide_index=True,
                                    column_config={"Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"), "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"), "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),}
                                )
        except Exception as e:
            if "does not exist" in str(e):
                 st.warning("Baza danych jest pusta lub nie została jeszcze utworzona. Przejdź do 'Panelu Admina', aby ją zainicjować.")
            else:
                 st.error(f"Wystąpił nieoczekiwany błąd w zakładce raportu: {e}")
                 st.exception(e) 

    # --- ZAKŁADKA 3: RENTOWNOŚĆ (NOWA LOGIKA ZAPISU PLIKU) ---
    with tab_rentownosc:
        st.header("Raport Rentowności (Zysk/Strata)")
        try:
            min_max_date_query = f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}"
            min_max_date = conn.query(min_max_date_query)
            
            if min_max_date.empty or min_max_date.iloc[0, 0] is None:
                st.info("Baza danych transakcji jest pusta. Ustawiam domyślne daty na dzisiaj.")
                domyslny_start_rent = date.today()
                domyslny_stop_rent = date.today()
                min_date_val = date(2020, 1, 1) 
                max_date_val = date.today()
            else:
                domyslny_start_rent = min_max_date.iloc[0, 0]
                domyslny_stop_rent = min_max_date.iloc[0, 1]
                min_date_val = domyslny_start_rent
                max_date_val = domyslny_stop_rent

            col1_rent, col2_rent = st.columns(2)
            with col1_rent:
                data_start_rent = st.date_input("Data Start", value=domyslny_start_rent, min_value=min_date_val, max_value=max_date_val, key="rent_start")
            with col2_rent:
                data_stop_rent = st.date_input("Data Stop", value=domyslny_stop_rent, min_value=min_date_val, max_value=max_date_val, key="rent_stop")
            
            st.divider()

            # --- NOWA LOGIKA WCZYTYWANIA I ZAPISYWANIA PLIKU ---
            
            plik_analizy = None 
            
            uploaded_file = st.file_uploader("Prześlij nowy plik `analiza.xlsx` (zastąpi zapisany)", type=['xlsx'])
            
            if uploaded_file is not None:
                plik_analizy = uploaded_file 
                st.info("Wykryto nowy plik. Zostanie użyty do generowania raportu.")
                if st.button("Zapisz ten plik na stałe (nadpisze stary)"):
                    zapisz_plik_w_bazie(conn, "analiza.xlsx", uploaded_file.getvalue())
            
            else:
                zapisany_plik_bytes = wczytaj_plik_z_bazy(conn, "analiza.xlsx") 
                if zapisany_plik_bytes is not None:
                    st.success("Używam pliku `analiza.xlsx` zapisanego w bazie.")
                    plik_analizy = io.BytesIO(zapisany_plik_bytes) 
                    if st.button("Usuń zapisany plik z bazy"):
                        usun_plik_z_bazy(conn, "analiza.xlsx")
                else:
                    st.warning("Brak zapisanego pliku `analiza.xlsx`. Musisz wgrać plik ręcznie, aby wygenerować raport.")
            
            st.divider()
            
            if 'raport_gotowy' not in st.session_state:
                st.session_state['raport_gotowy'] = False
            if 'wybrany_pojazd_rent' not in st.session_state:
                st.session_state['wybrany_pojazd_rent'] = "--- Wybierz pojazd ---"

            if st.button("Generuj raport rentowności", type="primary"):
                if plik_analizy is None:
                    st.error("Nie można wygenerować raportu. Brak pliku `analiza.xlsx` (ani wgranego, ani zapisanego w bazie).")
                    st.session_state['raport_gotowy'] = False 
                else:
                    with st.spinner("Pracuję..."):
                        dane_z_bazy_rent = pobierz_dane_z_bazy(conn, data_start_rent, data_stop_rent) 
                        dane_przygotowane_rent, _ = przygotuj_dane_paliwowe(dane_z_bazy_rent.copy())
                        st.session_state['dane_bazy_raw'] = dane_przygotowane_rent 
                        
                        if dane_przygotowane_rent is None: 
                            st.session_state['raport_gotowy'] = False
                            st.error("Nie udało się pobrać kursów walut z NBP.")
                        else:
                            # *** NOWA AGREGACJA Z BAZY: Sumujemy Netto i Brutto ***
                            df_koszty_baza_agg = dane_przygotowane_rent.groupby('identyfikator_clean').agg(
                                koszty_baza_netto=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                                koszty_baza_brutto=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                            )
                            
                            df_analiza_agreg, df_analiza_raw = przetworz_plik_analizy(plik_analizy, data_start_rent, data_stop_rent)
                            st.session_state['dane_analizy_raw'] = df_analiza_raw 
                            
                            if df_analiza_agreg is None:
                                if df_koszty_baza_agg.empty:
                                    st.error("Nie znaleziono żadnych danych ani w pliku 'analiza.xlsx', ani w bazie danych dla wybranego okresu.")
                                    st.session_state['raport_gotowy'] = False
                                    st.stop()
                                else:
                                    st.warning("Nie znaleziono danych w pliku 'analiza.xlsx' dla tych dat (lub brak przychodów), ale pokazuję koszty z bazy.")
                                    df_analiza_agreg = pd.DataFrame(columns=['przychody_brutto', 'przychody_netto', 'koszty_inne_brutto', 'koszty_inne_netto'])

                            # *** NOWE SCALANIE: Łączymy obie agregacje (Subiekt + Baza) ***
                            df_rentownosc = df_analiza_agreg.merge(
                                df_koszty_baza_agg, 
                                left_index=True, 
                                right_index=True, 
                                how='outer'
                            ).fillna(0)
                            
                            # *** NOWE OBLICZENIA ZYSKU: Brutto i Netto ***
                            df_rentownosc['ZYSK_STRATA_BRUTTO_EUR'] = (
                                df_rentownosc['przychody_brutto'] - 
                                df_rentownosc['koszty_inne_brutto'] - 
                                df_rentownosc['koszty_baza_brutto']
                            )
                            df_rentownosc['ZYSK_STRATA_NETTO_EUR'] = (
                                df_rentownosc['przychody_netto'] - 
                                df_rentownosc['koszty_inne_netto'] - 
                                df_rentownosc['koszty_baza_netto']
                            )
                            
                            st.session_state['raport_gotowy'] = True
                            st.session_state['df_rentownosc'] = df_rentownosc
                            st.session_state['wybrany_pojazd_rent'] = "--- Wybierz pojazd ---" 
                            
            if st.session_state.get('raport_gotowy', False):
                st.subheader("Wyniki dla wybranego okresu")
                
                # --- TUTAJ DODANO NOWY WYKRES DLA KONTRAHENTÓW ---
                df_analiza_raw = st.session_state.get('dane_analizy_raw')
                if df_analiza_raw is not None and not df_analiza_raw.empty:
                    st.write("### 🏢 Przychody wg Kontrahentów")
                    df_chart = df_analiza_raw[df_analiza_raw['typ'] == 'Przychód (Subiekt)'].copy()
                    
                    if not df_chart.empty:
                        # Wykres
                        chart_data = df_chart.groupby('kontrahent')['kwota_brutto_eur'].sum().sort_values(ascending=False)
                        st.bar_chart(chart_data)
                        
                        # --- NOWY FILTR KONTRAHENTOW (TABELA) ---
                        st.write("#### 🕵️ Szczegóły przychodów wg Kontrahenta")
                        lista_kontrahentow = sorted(df_chart['kontrahent'].unique().tolist())
                        wybrany_kontrahent_view = st.multiselect("Wybierz kontrahentów do tabeli:", lista_kontrahentow)
                        
                        if wybrany_kontrahent_view:
                            df_show = df_chart[df_chart['kontrahent'].isin(wybrany_kontrahent_view)]
                            st.dataframe(
                                df_show[['data', 'pojazd_clean', 'opis', 'kwota_netto_eur', 'kwota_brutto_eur']].style.format({
                                    'kwota_netto_eur': '{:,.2f} EUR', 
                                    'kwota_brutto_eur': '{:,.2f} EUR'
                                }),
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    "data": st.column_config.DateColumn("Data"),
                                    "pojazd_clean": "Pojazd",
                                    "opis": "Opis",
                                    "kwota_netto_eur": st.column_config.NumberColumn("Netto (EUR)"),
                                    "kwota_brutto_eur": st.column_config.NumberColumn("Brutto (EUR)")
                                }
                            )
                    else:
                        st.info("Brak danych przychodowych do wyświetlenia wykresu.")
                    st.divider()
                # ------------------------------------------------
                
                df_rentownosc = st.session_state['df_rentownosc']
                # Sortujemy po Brutto
                df_rentownosc = df_rentownosc.sort_values(by='ZYSK_STRATA_BRUTTO_EUR', ascending=False)
                
                lista_pojazdow_rent = ["--- Wybierz pojazd ---"] + list(df_rentownosc.index.unique())
                
                wybrany_pojazd_rent = st.selectbox(
                    "Wybierz pojazd do analizy (Pełny Rachunek Zysków/Strat):", 
                    lista_pojazdow_rent,
                    key='wybrany_pojazd_rent'
                )
                
                if wybrany_pojazd_rent != "--- Wybierz pojazd ---":
                    try:
                        dane_pojazdu = df_rentownosc.loc[wybrany_pojazd_rent]
                        # Pobieramy Brutto dla metryk
                        przychody = dane_pojazdu['przychody_brutto']
                        koszty_inne = dane_pojazdu['koszty_inne_brutto']
                        koszty_bazy = dane_pojazdu['koszty_baza_brutto']
                        zysk = dane_pojazdu['ZYSK_STRATA_BRUTTO_EUR']
                        
                        delta_color = "normal"
                        if zysk < 0: delta_color = "inverse"
                        
                        st.metric(label="ZYSK / STRATA (BRUTTO EUR)", value=f"{zysk:,.2f} EUR", delta_color=delta_color)
                        
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Przychód Brutto (Subiekt)", f"{przychody:,.2f} EUR")
                        col2.metric("Koszty Inne Brutto (Subiekt)", f"{-koszty_inne:,.2f} EUR")
                        col3.metric("Koszty Brutto z Bazy (Paliwo+Opłaty)", f"{-koszty_bazy:,.2f} EUR")
                    
                    except KeyError:
                        st.error("Nie znaleziono danych dla tego pojazdu.")

                    # --- *** POCZĄTEK NOWEJ FUNKCJI: SZCZEGÓŁOWA LISTA (NETTO/BRUTTO) *** ---
                    st.divider()
                    st.subheader(f"Szczegółowa lista transakcji dla {wybrany_pojazd_rent}")

                    df_analiza_raw = st.session_state.get('dane_analizy_raw')
                    dane_przygotowane_rent = st.session_state.get('dane_bazy_raw')
                    
                    lista_df_szczegolow = []
                    
                    # 1. Dane z pliku analizy (Subiekt)
                    if df_analiza_raw is not None and not df_analiza_raw.empty:
                        subiekt_details = df_analiza_raw[df_analiza_raw['pojazd_clean'] == wybrany_pojazd_rent].copy()
                        if not subiekt_details.empty:
                            subiekt_formatted = subiekt_details[['data', 'opis', 'zrodlo', 'kwota_netto_eur', 'kwota_brutto_eur']]
                            lista_df_szczegolow.append(subiekt_formatted)

                    # 2. Dane z bazy (Paliwo, Opłaty, Inne)
                    if dane_przygotowane_rent is not None and not dane_przygotowane_rent.empty:
                        baza_details = dane_przygotowane_rent[dane_przygotowane_rent['identyfikator_clean'] == wybrany_pojazd_rent].copy()
                        if not baza_details.empty:
                            baza_formatted = baza_details[['data_transakcji_dt', 'produkt', 'zrodlo', 'kwota_netto_eur', 'kwota_brutto_eur']].copy() 
                            baza_formatted['data_transakcji_dt'] = baza_formatted['data_transakcji_dt'].dt.date
                            
                            baza_formatted.rename(columns={
                                'data_transakcji_dt': 'data',
                                'produkt': 'opis'
                            }, inplace=True)
                            
                            baza_formatted['kwota_netto_eur'] = -baza_formatted['kwota_netto_eur'].abs() 
                            baza_formatted['kwota_brutto_eur'] = -baza_formatted['kwota_brutto_eur'].abs() 
                            lista_df_szczegolow.append(baza_formatted[['data', 'opis', 'zrodlo', 'kwota_netto_eur', 'kwota_brutto_eur']])

                    # 3. Połącz i wyświetl
                    if not lista_df_szczegolow:
                        st.info("Brak szczegółowych transakcji dla tego pojazdu w wybranym okresie.")
                    else:
                        combined_details = pd.concat(lista_df_szczegolow).sort_values(by='data', ascending=False)
                        
                        # --- POPRAWKA: RESET INDEKSU ABY UNIKNĄĆ DUPLIKATÓW ---
                        combined_details = combined_details.reset_index(drop=True)
                        # ------------------------------------------------------

                        def koloruj_kwoty(val):
                            if pd.isna(val): return ''
                            color = 'red' if val < 0 else 'green'
                            return f'color: {color}'
                        
                        st.dataframe(
                            combined_details.style.apply(axis=1, subset=['kwota_brutto_eur'], func=lambda row: [koloruj_kwoty(row.kwota_brutto_eur)]),
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "data": st.column_config.DateColumn("Data", format="YYYY-MM-DD"),
                                "opis": "Opis",
                                "zrodlo": "Źródło",
                                "kwota_netto_eur": st.column_config.NumberColumn("Netto (EUR)", format="%.2f EUR"),
                                "kwota_brutto_eur": st.column_config.NumberColumn("Brutto (EUR)", format="%.2f EUR")
                            }
                        )
                    # --- *** KONIEC NOWEJ FUNKCJI *** ---

                
                st.divider()
                # *** NOWE METRYKI SUMY: BRUTTO I NETTO ***
                zysk_laczny_brutto = df_rentownosc['ZYSK_STRATA_BRUTTO_EUR'].sum()
                zysk_laczny_netto = df_rentownosc['ZYSK_STRATA_NETTO_EUR'].sum()
                
                col_sum1, col_sum2 = st.columns(2)
                col_sum1.metric(label="SUMA ŁĄCZNA (ZYSK/STRATA BRUTTO)", value=f"{zysk_laczny_brutto:,.2f} EUR")
                col_sum2.metric(label="SUMA ŁĄCZNA (ZYSK/STRATA NETTO)", value=f"{zysk_laczny_netto:,.2f} EUR")
                
                
                # *** NOWA TABELA GŁÓWNA: POKAZUJE NETTO I BRUTTO ***
                st.subheader("Podsumowanie dla wszystkich pojazdów")
                df_rentownosc_display = df_rentownosc[[
                    'przychody_netto', 'przychody_brutto', 
                    'koszty_inne_netto', 'koszty_inne_brutto',
                    'koszty_baza_netto', 'koszty_baza_brutto',
                    'ZYSK_STRATA_NETTO_EUR', 'ZYSK_STRATA_BRUTTO_EUR'
                ]].rename(columns={
                    'przychody_netto': 'Przychód Netto (Subiekt)',
                    'przychody_brutto': 'Przychód Brutto (Subiekt)',
                    'koszty_inne_netto': 'Koszty Inne Netto (Subiekt)',
                    'koszty_inne_brutto': 'Koszty Inne Brutto (Subiekt)',
                    'koszty_baza_netto': 'Koszty Bazy Netto',
                    'koszty_baza_brutto': 'Koszty Bazy Brutto',
                    'ZYSK_STRATA_NETTO_EUR': 'ZYSK/STRATA NETTO',
                    'ZYSK_STRATA_BRUTTO_EUR': 'ZYSK/STRATA BRUTTO'
                })
                
                st.dataframe(
                    df_rentownosc_display.style.format("{:,.2f} EUR"),
                    use_container_width=True
                )
                st.download_button(
                    label="Pobierz raport rentowności jako Excel (.xlsx)",
                    data=to_excel(df_rentownosc_display),
                    file_name=f"raport_rentownosc_{data_start_rent}_do_{data_stop_rent}.xlsx",
                    mime="application/vnd.ms-excel"
                )

        except Exception as e:
            if "does not exist" in str(e):
                 if NAZWA_TABELI_PLIKOW in str(e):
                     st.warning("Tabela do zapisywania plików nie istnieje. Przejdź do 'Panelu Admina' i kliknij '2. Stwórz tabelę saved_files'.")
                 else:
                     st.warning("Baza danych transakcji jest pusta lub nie została jeszcze utworzona. Przejdź do 'Panelu Admina', aby ją zainicjować.")
            else:
                 st.error(f"Wystąpił nieoczekiwany błąd w zakładce raportu: {e}")
                 st.exception(e) 


# --- LOGIKA LOGOWANIA (BEZ ZMIAN) ---
def check_password():
    try:
        prawidlowe_haslo = st.secrets["ADMIN_PASSWORD"]
    except:
        st.error("Błąd krytyczny: Nie ustawiono 'ADMIN_PASSWORD' w Ustawieniach (Secrets) aplikacji.")
        st.stop()

    if 'raport_gotowy' not in st.session_state:
        st.session_state['raport_gotowy'] = False
    if 'wybrany_pojazd_rent' not in st.session_state:
        st.session_state['wybrany_pojazd_rent'] = "--- Wybierz pojazd ---"
        
    if st.session_state.get("password_correct", False):
        return True

    with st.form("login"):
        st.title("Logowanie")
        st.write("Wprowadź hasło, aby uzyskać dostęp do analizatora.")
        wpisane_haslo = st.text_input("Hasło", type="password")
        submitted = st.form_submit_button("Zaloguj")

        if submitted:
            if wpisane_haslo == prawidlowe_haslo:
                st.session_state["password_correct"] = True
                st.rerun() 
            else:
                st.error("Nieprawidłowe hasło.")
    return False

# --- GŁÓWNE URUCHOMIENIE APLIKACJI ---
if check_password():
    main_app()
