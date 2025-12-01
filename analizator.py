import pandas as pd
import numpy as np
import requests
import re
import streamlit as st
import time
from datetime import date
from sqlalchemy import text
import io
import os

# --- USTAWIENIA STRONY ---
icon_image = "⛟" 

st.set_page_config(
    page_title="Analizator Wydatków Multi-Firma",
    layout="wide",
    page_icon=icon_image 
)

# --- STYLIZACJA CSS ---
st.markdown("""
    <style>
    footer {visibility: hidden;}
    .main .block-container {padding-top: 2rem;}
    div[data-testid="stSidebar"] div.stButton > button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
        border: 1px solid #4b4b4b;
    }
    div[data-testid="stSidebar"] div.stButton > button[kind="primary"] {
        background-color: #44475a;
        border-color: #6272a4;
        color: white;
        font-weight: bold;
    }
    div[data-testid="stSidebar"] div.stButton > button[kind="primary"]:hover {
        background-color: #50546b;
        border-color: #7083b7;
    }
    div[data-testid="stSidebar"] div.stButton > button[kind="secondary"] {
        background-color: transparent;
        border-color: transparent;
        color: #b0b0b0;
    }
    div[data-testid="stSidebar"] div.stButton > button[kind="secondary"]:hover {
        background-color: #2b2b2b;
        color: white;
    }
    hr {
        margin-top: 1em;
        margin-bottom: 1em;
        border-color: #4b4b4b;
    }
    </style>
""", unsafe_allow_html=True)

# --- PARAMETRY TABELI ---
NAZWA_TABELI = "transactions"
NAZWA_TABELI_PLIKOW = "saved_files"
NAZWA_SCHEMATU = "public"
NAZWA_POLACZENIA_DB = "db"

# --- LISTA FIRM ---
FIRMY = ["HOLIER", "UNIX-TRANS"]

# --- KONFIGURACJA DLA UNIX-TRANS ---
UNIX_FLOTA_CONFIG = {
    'WGM8463A': date(2025, 10, 8),
    'WPR9335N': date(2025, 10, 7),
    'PTU3287F': date(2025, 11, 19),
    'WPR9685N': date(2025, 10, 21),
    # 'NOL0935C': date(2025, 10, 1), 
}

UNIX_ALIAS_MAPPING = {
    'TRUCK3': 'WGM8463A',        
    'PLTRUCK3': 'WGM8463A',      
    'PTU0002': 'WGM8463A',      
}

# --- SŁOWNIK VAT ---
VAT_RATES = {
    "PL": 0.23, "DE": 0.19, "CZ": 0.21, "AT": 0.20, "FR": 0.20,
    "DK": 0.25, "NL": 0.21, "BE": 0.21, "ES": 0.21, "IT": 0.22,
    "LT": 0.21, "LV": 0.21, "EE": 0.20, "SK": 0.20, "HU": 0.27,
    "RO": 0.19, "BG": 0.20, "SI": 0.22, "HR": 0.25, "SE": 0.25
}

# --- LISTY DO PARSOWANIA ---
ETYKIETY_PRZYCHODOW = [
    'Faktura VAT sprzedaży', 
    'Przychód wewnętrzny', 
    'Rachunek sprzedaży',
    'Korekta faktury VAT sprzedaży',
    'Paragon',
    'Paragon imienny'
]

ETYKIETY_KOSZTOW_INNYCH = [
    'Faktura VAT zakupu', 
    'Korekta faktury VAT zakupu', 
    'Rachunek zakupu',
    'Tankowanie',                
    'Paliwo',
    'Opłata drogowa',            
    'Opłaty drogowe',
    'Opłata drogowa DK',
    'Art. biurowe', 'Art. chemiczne', 'Art. spożywcze', 'Badanie lekarskie', 'Delegacja', 
    'Giełda', 'Księgowość', 'Leasing', 'Mandaty', 'Obsługa prawna', 
    'Ogłoszenie', 'Poczta Polska', 'Program', 'Prowizje', 
    'Rozliczanie kierowców', 'Rozliczenie VAT EUR', 'Serwis', 'Szkolenia BHP', 
    'Tachograf', 'USŁ. HOTELOWA', 'Usługi telekomunikacyjne', 'Wykup auta', 
    'Wysyłka kurierska', 'Zak. do auta', 'Zakup auta', 'Części', 'Myjnia',
    'Ubezpieczenie'
]

ETYKIETY_IGNOROWANE = [
    'Zamówienie od klienta', 
    'Wydanie zewnętrzne',        
    'Oferta', 
    'Proforma',
    'Suma końcowa', 
    'Nr pojazdu'
]

WSZYSTKIE_ZNANE_ETYKIETY = ETYKIETY_PRZYCHODOW + ETYKIETY_KOSZTOW_INNYCH + ETYKIETY_IGNOROWANE

# --- KONFIGURACJA FILTRÓW ---
ZAKAZANE_POJAZDY_LISTA = [
       
    'HEROSTALSP',
    'KUEHNE',
    'GRUPAKAPITA',
    'REGRINDSP',
    'PTU0001',        
    'PTU0002',
    'ZALICZKA'
]

def czy_zakazany_pojazd_global(nazwa):
    if not nazwa: return False
    n = str(nazwa).upper().replace(" ", "").replace("-", "")
    
    if n in UNIX_ALIAS_MAPPING:
        return False
        
    for zakazany in ZAKAZANE_POJAZDY_LISTA:
        if zakazany in n:
            return True
    return False

# --- FUNKCJE NBP ---
@st.cache_data
def pobierz_kurs_eur_pln():
    try:
        url = 'http://api.nbp.pl/api/exchangerates/rates/a/eur/?format=json'
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        kurs = response.json()['rates'][0]['mid']
        return kurs
    except requests.exceptions.RequestException as e:
        return 4.30

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

# --- KATEGORYZACJA TRANSAKCJI ---
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
    
    elif zrodlo == 'Fakturownia':
        nip_sprzedawcy = str(row.get('NIP sprzedającego', '')).replace('-', '').strip()
        nip_nabywcy = str(row.get('NIP', '')).replace('-', '').strip() 
        
        unix_nip = '9691670149'

        if unix_nip in nip_sprzedawcy:
             return 'PRZYCHÓD', str(row.get('Produkt/usługa', 'Usługa Transportowa'))
            
        if unix_nip in nip_nabywcy:
             return 'KOSZT', str(row.get('Produkt/usługa', 'Koszt'))

        sprzedawca = str(row.get('Sprzedający', '')).upper()
        nabywca = str(row.get('Nabywca', '')).upper()
        
        if 'UNIX' in sprzedawca:
             return 'PRZYCHÓD', str(row.get('Produkt/usługa', 'Usługa Transportowa'))
        if 'UNIX' in nabywca:
             return 'KOSZT', str(row.get('Produkt/usługa', 'Koszt'))
        
        return 'IGNORUJ', 'Nieznane'
        
    return 'INNE', 'Nieznane'
    
# --- NORMALIZACJA ---
def normalizuj_eurowag(df_eurowag, firma_tag):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_eurowag['Data i godzina'], errors='coerce')
    df_out['identyfikator'] = df_eurowag['Tablica rejestracyjna'].fillna(df_eurowag['Posiadacz karty'].fillna(df_eurowag['Karta']))
    df_out['kwota_netto'] = pd.to_numeric(df_eurowag['Kwota netto'], errors='coerce')
    df_out['kwota_brutto'] = pd.to_numeric(df_eurowag['Kwota brutto'], errors='coerce')
    df_out['waluta'] = df_eurowag['Waluta']
    df_out['ilosc'] = pd.to_numeric(df_eurowag['Ilość'], errors='coerce')
    
    if 'Kraj' in df_eurowag.columns:
        df_out['kraj'] = df_eurowag['Kraj'].str.upper().str.strip()
    else:
        df_out['kraj'] = 'Nieznany'

    kategorie = df_eurowag.apply(lambda row: kategoryzuj_transakcje(row, 'Eurowag'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie]
    df_out['produkt'] = [kat[1] for kat in kategorie]
    df_out['zrodlo'] = 'Eurowag'
    df_out['firma'] = firma_tag

    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

# --- NORMALIZACJA E100 PL (Z POPRAWKĄ NA KARTĘ KOŃCÓWKA 24) ---
def normalizuj_e100_PL(df_e100, firma_tag):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_e100['Data'] + ' ' + df_e100['Czas'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    
    # --- LOGIKA PRZYPISYWANIA POJAZDU PO KARCIE ---
    def ustal_identyfikator(row):
        nr_karty = str(row.get('Numer karty', '')).strip()
        nr_samochodu = str(row.get('Numer samochodu', '')).strip()
        
        # 1. WYJĄTEK DLA WGM (Karta końcówka 24)
        if nr_karty.endswith('24'):
            return 'WGM8463A'
            
        # 2. RESZTA POJAZDÓW (Zwracamy to co jest w pliku, np. drugi TRUCK)
        if nr_samochodu and nr_samochodu.lower() != 'nan':
            return nr_samochodu
        return nr_karty

    df_out['identyfikator'] = df_e100.apply(ustal_identyfikator, axis=1)
    # -----------------------------------------------
    
    kwota_brutto = pd.to_numeric(df_e100['Kwota'], errors='coerce')
    vat_rate = df_e100['Kraj'].map(VAT_RATES).fillna(0.0) 
    df_out['kwota_netto'] = kwota_brutto / (1 + vat_rate)
    df_out['kwota_brutto'] = kwota_brutto
    
    df_out['waluta'] = df_e100['Waluta']
    df_out['ilosc'] = pd.to_numeric(df_e100['Ilość'], errors='coerce')
    
    if 'Kraj' in df_e100.columns:
        df_out['kraj'] = df_e100['Kraj'].str.upper().str.strip()
    else:
        df_out['kraj'] = 'PL' 

    kategorie = df_e100.apply(lambda row: kategoryzuj_transakcje(row, 'E100_PL'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie]
    df_out['produkt'] = [kat[1] for kat in kategorie]
    df_out['zrodlo'] = 'E100_PL'
    df_out['firma'] = firma_tag
    
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

# --- NORMALIZACJA E100 EN ---
def normalizuj_e100_EN(df_e100, firma_tag):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_e100['Date'] + ' ' + df_e100['Time'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    
    def ustal_identyfikator_en(row):
        nr_karty = str(row.get('Card number', '')).strip()
        nr_samochodu = str(row.get('Car registration number', '')).strip()
        
        # Wyjątek dla karty kończącej się na 24
        if nr_karty.endswith('24'):
            return 'WGM8463A'
        
        # Reszta bez zmian
        if nr_samochodu and nr_samochodu.lower() != 'nan':
            return nr_samochodu
        return nr_karty

    df_out['identyfikator'] = df_e100.apply(ustal_identyfikator_en, axis=1)
    
    kwota_brutto = pd.to_numeric(df_e100['Sum'], errors='coerce')
    vat_rate = df_e100['Country'].map(VAT_RATES).fillna(0.0) 
    df_out['kwota_netto'] = kwota_brutto / (1 + vat_rate)
    df_out['kwota_brutto'] = kwota_brutto
    
    df_out['waluta'] = df_e100['Currency']
    df_out['ilosc'] = pd.to_numeric(df_e100['Quantity'], errors='coerce')

    if 'Country' in df_e100.columns:
        df_out['kraj'] = df_e100['Country'].str.upper().str.strip()
    else:
        df_out['kraj'] = 'Nieznany'

    kategorie = df_e100.apply(lambda row: kategoryzuj_transakcje(row, 'E100_EN'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie]
    df_out['produkt'] = [kat[1] for kat in kategorie] 
    df_out['zrodlo'] = 'E100_EN'
    df_out['firma'] = firma_tag
    
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out
# --- NORMALIZACJA FAKTUROWNI ---
def normalizuj_fakturownia(df_fakt, firma_tag):
    df = df_fakt.copy()
    
    col_map = {}
    for c in df.columns:
        c_lower = str(c).lower().strip()
        if 'cena' in c_lower and 'netto' in c_lower and 'pln' not in c_lower:
            col_map[c] = 'Cena netto'
        elif 'cena' in c_lower and 'brutto' in c_lower and 'pln' not in c_lower:
            col_map[c] = 'Cena brutto'
        elif 'ilość' in c_lower or 'ilosc' in c_lower:
            col_map[c] = 'Ilość'
        elif 'sprzedaj' in c_lower and 'nip' not in c_lower:
            col_map[c] = 'Sprzedający'
        elif 'nip' in c_lower and 'sprzed' in c_lower:
            col_map[c] = 'NIP sprzedającego'
        elif 'nabywca' in c_lower and 'nip' not in c_lower:
            col_map[c] = 'Nabywca'
        elif 'data wyst' in c_lower:
            col_map[c] = 'Data wystawienia'
        elif 'produkt' in c_lower or 'usługa' in c_lower:
             col_map[c] = 'Produkt/usługa'
        elif 'waluta' in c_lower:
             col_map[c] = 'Waluta'

    df.rename(columns=col_map, inplace=True)
    df = df.loc[:, ~df.columns.duplicated()]

    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df['Data wystawienia'], errors='coerce')
    
    if 'Nabywca' in df.columns:
        df_out['kontrahent'] = df['Nabywca']
    else:
        df_out['kontrahent'] = 'Brak Kontrahenta'
    
    def znajdz_pojazd(row):
        cols_to_search = ['Uwagi', 'Nr zamówienia', 'Opis', 'Dodatkowe pole na pozycjach faktury', 'Produkt/usługa']
        text_full = ""
        for col in cols_to_search:
            if col in row and pd.notna(row[col]):
                text_full += " " + str(row[col])
        
        match = re.search(r'\b[A-Z]{2,3}[\s-]?[0-9A-Z]{4,5}\b', text_full.upper())
        if match:
            found = match.group(0).replace(" ", "").replace("-", "")
            if found not in ['POLSKA', 'PRZELEW', 'BANK', 'FAKTURA', 'TRANS', 'LOGISTICS']:
                return found
        return 'Brak Pojazdu'

    df_out['identyfikator'] = df.apply(znajdz_pojazd, axis=1)

    if 'Cena netto' in df.columns and 'Ilość' in df.columns:
        cena_netto = pd.to_numeric(df['Cena netto'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0.0)
        ilosc = pd.to_numeric(df['Ilość'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0.0)
        
        df_out['kwota_netto'] = cena_netto * ilosc
        
        if 'Cena brutto' in df.columns:
            cena_brutto = pd.to_numeric(df['Cena brutto'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0.0)
            df_out['kwota_brutto'] = cena_brutto * ilosc
        else:
            df_out['kwota_brutto'] = df_out['kwota_netto'] 
    else:
        if 'Wartość netto' in df.columns:
             df_out['kwota_netto'] = pd.to_numeric(df['Wartość netto'].astype(str).str.replace(',', '.'), errors='coerce')
        if 'Wartość brutto' in df.columns:
             df_out['kwota_brutto'] = pd.to_numeric(df['Wartość brutto'].astype(str).str.replace(',', '.'), errors='coerce')

    df_out['waluta'] = df.get('Waluta', 'PLN')
    df_out['ilosc'] = 1.0 
    
    if 'Kraj' in df.columns:
        df_out['kraj'] = df['Kraj'].fillna('PL')
    else:
        df_out['kraj'] = 'PL'

    kategorie = df.apply(lambda row: kategoryzuj_transakcje(row, 'Fakturownia'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie] 
    df_out['produkt'] = [kat[1] for kat in kategorie]
    df_out['zrodlo'] = 'Fakturownia'
    df_out['firma'] = firma_tag

    df_out = df_out.dropna(subset=['data_transakcji'])
    df_out = df_out[df_out['typ'] != 'IGNORUJ']
    df_out = df_out[df_out['kwota_brutto'] != 0]
    
    return df_out

# --- WCZYTYWANIE PLIKÓW ---
def wczytaj_i_zunifikuj_pliki(przeslane_pliki, wybrana_firma_upload):
    lista_df_zunifikowanych = []
    
    for plik in przeslane_pliki:
        nazwa_pliku_base = plik.name
        st.write(f" - Przetwarzam: {nazwa_pliku_base} (Firma: {wybrana_firma_upload})")
        
        try:
            plik_bytes = plik.getvalue()
        except Exception as e:
            st.error(f"Nie udało się pobrać zawartości pliku: {e}")
            continue

        sukces_pliku = False

        if nazwa_pliku_base.lower().endswith(('.xls', '.xlsx')):
            try:
                xls = pd.ExcelFile(io.BytesIO(plik_bytes), engine='openpyxl')
                
                if 'Transactions' in xls.sheet_names:
                    df_e100 = pd.read_excel(xls, sheet_name='Transactions')
                    col_e100 = df_e100.columns
                    if 'Numer samochodu' in col_e100 and 'Kwota' in col_e100:
                        st.write("    -> Wykryto format E100 (Polski - Excel)")
                        lista_df_zunifikowanych.append(normalizuj_e100_PL(df_e100, wybrana_firma_upload))
                        sukces_pliku = True
                    elif 'Car registration number' in col_e100 and 'Sum' in col_e100:
                        st.write("    -> Wykryto format E100 (Angielski - Excel)")
                        lista_df_zunifikowanych.append(normalizuj_e100_EN(df_e100, wybrana_firma_upload))
                        sukces_pliku = True
                
                elif 'Sheet0' in xls.sheet_names or len(xls.sheet_names) > 0:
                    df_check = pd.read_excel(xls, sheet_name=0)
                    cols = df_check.columns
                    if 'Data i godzina' in cols and ('Posiadacz karty' in cols or 'Artykuł' in cols):
                         st.write("    -> Wykryto format Eurowag (Excel)")
                         if 'Posiadacz karty' not in cols: df_check['Posiadacz karty'] = None
                         lista_df_zunifikowanych.append(normalizuj_eurowag(df_check, wybrana_firma_upload))
                         sukces_pliku = True
                    elif any('Sprzedaj' in c for c in cols) and any('Nabywca' in c for c in cols):
                         st.write("    -> Wykryto format Fakturownia (Prawdziwy Excel)")
                         lista_df_zunifikowanych.append(normalizuj_fakturownia(df_check, wybrana_firma_upload))
                         sukces_pliku = True
            except Exception:
                pass 

        if sukces_pliku:
            continue

        separators = [',', ';', '\t']
        encodings = ['utf-8', 'cp1250', 'latin1', 'utf-8-sig']
        df_csv = None
        
        for enc in encodings:
            for sep in separators:
                try:
                    buffer = io.BytesIO(plik_bytes)
                    temp_df = pd.read_csv(buffer, sep=sep, encoding=enc, engine='python', on_bad_lines='skip')
                    cols = temp_df.columns
                    is_fakt = any('Sprzedaj' in c for c in cols) or any('NIP' in c for c in cols) or any('Data wyst' in c for c in cols)
                    if is_fakt: 
                        df_csv = temp_df
                        st.write(f"    -> Wczytano jako CSV (Kodowanie: {enc}, Separator: '{sep}')")
                        break
                except Exception:
                    continue
            if df_csv is not None:
                break
        
        if df_csv is not None:
            st.write("    -> Wykryto format Fakturownia (CSV)")
            lista_df_zunifikowanych.append(normalizuj_fakturownia(df_csv, wybrana_firma_upload))
            sukces_pliku = True

        if sukces_pliku:
            continue

        if not sukces_pliku:
            st.error(f"Nie udało się rozpoznać formatu pliku: {nazwa_pliku_base}")

    if not lista_df_zunifikowanych:
        return None, "Nie udało się zunifikować żadnych danych."
        
    polaczone_df = pd.concat(lista_df_zunifikowanych, ignore_index=True)
    return polaczone_df, None

# --- FUNKCJE BAZY DANYCH ---
def setup_database(conn):
    with conn.session as s:
        s.execute(text(f"DROP TABLE IF EXISTS {NAZWA_SCHEMATU}.{NAZWA_TABELI}"))
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
                zrodlo VARCHAR(50),
                kraj VARCHAR(50),
                firma VARCHAR(50),
                kontrahent VARCHAR(255)
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
              AND a.firma = b.firma
        );
        """))
        s.commit()

def pobierz_dane_z_bazy(conn, data_start, data_stop, wybrana_firma, typ=None):
    params = {"data_start": data_start, "data_stop": data_stop, "firma": wybrana_firma}
    base_query = f"""
        SELECT * FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}
        WHERE (data_transakcji::date) >= :data_start 
          AND (data_transakcji::date) <= :data_stop
    """
    if wybrana_firma == "UNIX-TRANS":
        condition = """
          AND (
            firma = :firma 
            OR (zrodlo IN ('Eurowag', 'E100_PL', 'E100_EN') AND firma = 'HOLIER')
          )
        """
    else:
        condition = " AND firma = :firma"

    query = base_query + condition
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
        if key == 'nan' or not key or key == 'Brak Pojazdu': 
            return 'Brak Identyfikatora'
            
        key_nospace = key.upper().replace(" ", "").replace("-", "").strip().strip('"')
        
        if key_nospace in UNIX_ALIAS_MAPPING:
            return UNIX_ALIAS_MAPPING[key_nospace]
            
        FIRMY_DO_USUNIECIA = [
            'TRUCK24SP', 'TRUCK24', 'EDENRED', 'MARMAR', 'SANTANDER', 
            'LEASING', 'PZU', 'WARTA', 'INTERCARS', 'EUROWAG', 'E100', 'POLSKA', 'BANK'
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

# --- PRZYGOTOWANIE DANYCH ---
def przygotuj_dane_paliwowe(dane_z_bazy, firma_kontekst=None):
    if dane_z_bazy.empty:
        return dane_z_bazy, None
    
    dane_z_bazy = dane_z_bazy[dane_z_bazy['zrodlo'] != 'Fakturownia']

    if dane_z_bazy.empty:
        return dane_z_bazy, None

    dane_z_bazy['data_transakcji_dt'] = pd.to_datetime(dane_z_bazy['data_transakcji'])
    dane_z_bazy['identyfikator_clean'] = bezpieczne_czyszczenie_klucza(dane_z_bazy['identyfikator'])
    
    # --- FILTR DLA UNIX-TRANS ---
    if firma_kontekst == "UNIX-TRANS":
        def filter_unix(row):
            pojazd = str(row['identyfikator_clean']).upper().replace(" ", "").replace("-", "")
            
            # Jeśli firma to UNIX-TRANS, sprawdzamy czy auto jest we flocie UNIXa
            if row['firma'] == 'UNIX-TRANS':
                if pojazd in UNIX_FLOTA_CONFIG:
                    return True
                else:
                    return False
            
            # Jeśli firma to HOLIER (ale płacił Unix przez udostępnienie), sprawdzamy czy to auto UNIXa
            if row['firma'] == 'HOLIER':
                if pojazd in UNIX_FLOTA_CONFIG:
                    data_tr = row['data_transakcji_dt'].date()
                    start_date = UNIX_FLOTA_CONFIG[pojazd]
                    if data_tr >= start_date:
                        return True
            return False
        maska = dane_z_bazy.apply(filter_unix, axis=1)
        dane_z_bazy = dane_z_bazy[maska]
    
    # --- FILTR DLA HOLIER ---
    elif firma_kontekst == "HOLIER":
        def filter_holier(row):
            pojazd = str(row['identyfikator_clean']).upper().replace(" ", "").replace("-", "")
            # Ukrywamy koszty aut UNIXa, które są przypisane do UNIXa
            if pojazd in UNIX_FLOTA_CONFIG:
                data_tr = row['data_transakcji_dt'].date()
                start_date = UNIX_FLOTA_CONFIG[pojazd]
                if data_tr >= start_date:
                    return False # Ukryj, bo jest w Unixie
            return True
        maska = dane_z_bazy.apply(filter_holier, axis=1)
        dane_z_bazy = dane_z_bazy[maska]

    if dane_z_bazy.empty:
        return dane_z_bazy, None

    if 'kraj' not in dane_z_bazy.columns:
        dane_z_bazy['kraj'] = 'Nieznany'
    
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

# --- LOGIKA REFAKTUR ---
def pobierz_dane_do_refaktury(conn, data_start, data_stop):
    # Pobieramy wszystko z bazy w zakresie dat
    df_all = pobierz_dane_z_bazy(conn, data_start, data_stop, "UNIX-TRANS") # Używamy UNIX-TRANS żeby pobrać szeroki zakres (z logiką OR)
    if df_all.empty: return None, None, None, None
    
    df_all = df_all[df_all['zrodlo'] != 'Fakturownia']
    df_all['data_transakcji_dt'] = pd.to_datetime(df_all['data_transakcji'])
    df_all['identyfikator_clean'] = bezpieczne_czyszczenie_klucza(df_all['identyfikator'])
    
    kurs_eur = pobierz_kurs_eur_pln()
    if not kurs_eur: return None, None, None, None
    mapa_kursow = pobierz_wszystkie_kursy(df_all['waluta'].unique(), kurs_eur)
    
    df_all['kwota_netto_num'] = pd.to_numeric(df_all['kwota_netto'], errors='coerce').fillna(0.0)
    df_all['kwota_brutto_num'] = pd.to_numeric(df_all['kwota_brutto'], errors='coerce').fillna(0.0)
    df_all['kwota_netto_eur'] = df_all.apply(lambda row: row['kwota_netto_num'] * mapa_kursow.get(row['waluta'], 0.0), axis=1)
    df_all['kwota_brutto_eur'] = df_all.apply(lambda row: row['kwota_brutto_num'] * mapa_kursow.get(row['waluta'], 0.0), axis=1)

    # 1. KIERUNEK: HOLIER -> UNIX (Unix winien Holierowi)
    def filter_holier_to_unix(row):
        pojazd = str(row['identyfikator_clean']).upper().replace(" ", "").replace("-", "")
        if row['firma'] == 'HOLIER' and pojazd in UNIX_FLOTA_CONFIG:
            data_tr = row['data_transakcji_dt'].date()
            start_date = UNIX_FLOTA_CONFIG[pojazd]
            if data_tr >= start_date:
                return True
        return False

    # 2. KIERUNEK: UNIX -> HOLIER (Holier winien Unixowi)
    def filter_unix_to_holier(row):
        pojazd = str(row['identyfikator_clean']).upper().replace(" ", "").replace("-", "")
        if row['firma'] == 'UNIX-TRANS':
            if pojazd not in UNIX_FLOTA_CONFIG:
                return True # To auto obce (Holiera), za które zapłacił Unix
        return False

    maska_h_to_u = df_all.apply(filter_holier_to_unix, axis=1)
    df_holier_to_unix = df_all[maska_h_to_u]
    
    maska_u_to_h = df_all.apply(filter_unix_to_holier, axis=1)
    df_unix_to_holier = df_all[maska_u_to_h]

    return df_holier_to_unix, df_unix_to_holier, mapa_kursow

# --- PARSOWANIE ANALIZY ---
@st.cache_data 
def przetworz_plik_analizy(przeslany_plik_bytes, data_start, data_stop, wybrana_firma):
    if wybrana_firma == "UNIX-TRANS":
         df_csv = None
         try:
             plik_content = przeslany_plik_bytes.getvalue()
         except:
             plik_content = przeslany_plik_bytes 

         encodings_to_try = ['utf-8', 'utf-8-sig', 'cp1250', 'latin1']
         separators = [',', ';', '\t']
         
         for enc in encodings_to_try:
             for sep in separators:
                 try:
                     temp_df = pd.read_csv(io.BytesIO(plik_content), sep=sep, engine='python', encoding=enc, on_bad_lines='skip')
                     cols = temp_df.columns
                     is_fakt = any('Sprzedaj' in c for c in cols) or any('NIP' in c for c in cols)
                     if is_fakt:
                         df_csv = temp_df
                         break
                 except:
                     continue
             if df_csv is not None:
                 break
         
         if df_csv is None:
             try:
                 df_csv = pd.read_excel(io.BytesIO(plik_content))
             except:
                 pass

         if df_csv is None:
             st.error("Nie udało się odczytać pliku analizy UNIX (Fakturownia).")
             return None, None

         try:
             df_zunifikowane = normalizuj_fakturownia(df_csv, "UNIX-TRANS")
             
             kurs_eur = pobierz_kurs_eur_pln()
             if not kurs_eur: return None, None
             mapa_kursow = pobierz_wszystkie_kursy(df_zunifikowane['waluta'].unique(), kurs_eur)
             
             df_zunifikowane['kwota_netto_eur'] = df_zunifikowane.apply(lambda row: row['kwota_netto'] * mapa_kursow.get(row['waluta'], 0.0), axis=1)
             df_zunifikowane['kwota_brutto_eur'] = df_zunifikowane.apply(lambda row: row['kwota_brutto'] * mapa_kursow.get(row['waluta'], 0.0), axis=1)

             df_zunifikowane = df_zunifikowane[
                (df_zunifikowane['data_transakcji'].dt.date >= data_start) & 
                (df_zunifikowane['data_transakcji'].dt.date <= data_stop)
             ]
             
             df_wyniki = df_zunifikowane.copy()
             df_wyniki['pojazd_clean'] = bezpieczne_czyszczenie_klucza(df_wyniki['identyfikator'])
             
             # --- FILTR ZALICZEK ---
             maska_zaliczka_pojazd = df_wyniki['pojazd_clean'].astype(str).str.contains('ZALICZKA', case=False, na=False)
             maska_zaliczka_opis = df_wyniki['produkt'].astype(str).str.contains('ZALICZKA', case=False, na=False)
             df_wyniki = df_wyniki[~(maska_zaliczka_pojazd | maska_zaliczka_opis)]
             
             df_wyniki['typ'] = df_wyniki['typ'].fillna('Koszt (Subiekt)')
             df_wyniki['typ'] = df_wyniki['typ'].replace({
                 'PRZYCHÓD': 'Przychód (Subiekt)', 
                 'KOSZT': 'Koszt (Subiekt)'
             })
             
             df_wyniki['opis'] = df_wyniki['produkt']
             df_wyniki['data'] = df_wyniki['data_transakcji'].dt.date
             
             if 'kontrahent' not in df_wyniki.columns:
                 df_wyniki['kontrahent'] = 'Brak Kontrahenta'
             else:
                 df_wyniki['kontrahent'] = df_wyniki['kontrahent'].fillna('Brak Kontrahenta')
             
             df_przychody = df_wyniki[df_wyniki['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kwota_brutto_eur'].sum().to_frame('przychody_brutto')
             df_przychody_netto = df_wyniki[df_wyniki['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kwota_netto_eur'].sum().to_frame('przychody_netto')
             
             df_koszty = df_wyniki[df_wyniki['typ'] == 'Koszt (Subiekt)'].groupby('pojazd_clean')['kwota_brutto_eur'].sum().to_frame('koszty_inne_brutto')
             df_koszty_netto = df_wyniki[df_wyniki['typ'] == 'Koszt (Subiekt)'].groupby('pojazd_clean')['kwota_netto_eur'].sum().to_frame('koszty_inne_netto')

             df_agregacja = pd.concat([df_przychody, df_przychody_netto, df_koszty, df_koszty_netto], axis=1).fillna(0)
             return df_agregacja, df_wyniki

         except Exception as e:
             st.error(f"Błąd przetwarzania danych Unix: {e}")
             return None, None

    # --- KROK 2: SCIEŻKA DLA HOLIER (SUBIEKT EXCEL) ---
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
            return None, None
        lista_iso_walut = list(MAPA_WALUT_PLIKU.values())
        mapa_kursow = pobierz_wszystkie_kursy(lista_iso_walut, kurs_eur_pln_nbp)
    except Exception as e:
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
                MAPA_BRUTTO_DO_KURSU[(col_waluta, col_typ)] = (kurs, iso_code)
            if col_waluta in MAPA_WALUT_PLIKU and col_typ == TYP_KWOTY_NETTO:
                iso_code = MAPA_WALUT_PLIKU[col_waluta]
                kurs = mapa_kursow.get(iso_code, 0.0)
                if iso_code == 'EUR': kurs = 1.0
                MAPA_NETTO_DO_KURSU[(col_waluta, col_typ)] = (kurs, iso_code)
    except Exception as e:
        st.error(f"Nie udało się wczytać pliku Excel Holier. Błąd: {e}")
        return None, None

    wyniki = []
    lista_aktualnych_pojazdow = [] 
    aktualny_kontrahent = None 
    ostatnia_etykieta_pojazdu = None
    aktualna_data = None                  
    date_regex = re.compile(r'^\d{4}-\d{2}-\d{2}$') 
    
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
            'ZALICZKA' 
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
        if not has_vehicle_word: return False
        for word in words:
            if len(word) > 12: return False
        return True

    for index, row in df.iterrows():
        try:
            etykieta_wiersza = str(row[kolumna_etykiet_tuple]).strip()
            kwota_brutto_eur = 0.0
            kwota_netto_eur = 0.0
            znaleziona_waluta = "EUR"
            znaleziona_kwota_org = 0.0
            found_orig = False
            for col_tuple, (kurs, iso_code) in MAPA_BRUTTO_DO_KURSU.items():
                if pd.notna(row[col_tuple]):
                    kwota_val = pd.to_numeric(row[col_tuple], errors='coerce')
                    if pd.isna(kwota_val): kwota_val = 0.0
                    if kwota_val != 0.0:
                        kwota_brutto_eur += kwota_val * kurs
                        if not found_orig:
                            znaleziona_waluta = iso_code
                            znaleziona_kwota_org = kwota_val
                            found_orig = True
            for col_tuple, (kurs, iso_code) in MAPA_NETTO_DO_KURSU.items():
                 if pd.notna(row[col_tuple]):
                    kwota_val = pd.to_numeric(row[col_tuple], errors='coerce')
                    if pd.isna(kwota_val): kwota_val = 0.0
                    kwota_netto_eur += kwota_val * kurs
            kwota_laczna = kwota_brutto_eur if kwota_brutto_eur != 0 else kwota_netto_eur
        except Exception as e_row:
            continue 

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

        elif etykieta_wiersza in WSZYSTKIE_ZNANE_ETYKIETY:
            if etykieta_wiersza in ETYKIETY_IGNOROWANE:
                continue 
            ostatnia_etykieta_pojazdu = etykieta_wiersza
            if kwota_laczna != 0.0:
                etykieta_do_uzycia = ostatnia_etykieta_pojazdu
                kwota_netto_do_uzycia = kwota_netto_eur
                kwota_brutto_do_uzycia = kwota_brutto_eur
                waluta_do_uzycia = znaleziona_waluta
                kwota_org_do_uzycia = znaleziona_kwota_org
                ostatnia_etykieta_pojazdu = None 
            else:
                continue

        elif (etykieta_wiersza == 'nan' or not etykieta_wiersza) and kwota_laczna != 0.0:
            if ostatnia_etykieta_pojazdu: 
                etykieta_do_uzycia = ostatnia_etykieta_pojazdu
                kwota_netto_do_uzycia = kwota_netto_eur
                kwota_brutto_do_uzycia = kwota_brutto_eur
                waluta_do_uzycia = znaleziona_waluta
                kwota_org_do_uzycia = znaleziona_kwota_org
                ostatnia_etykieta_pojazdu = None 
            else:
                continue 

        elif etykieta_wiersza != 'nan' and etykieta_wiersza:
            if is_vehicle_line(etykieta_wiersza):
                lista_aktualnych_pojazdow = re.split(r'\s+i\s+|\s+I\s+|\s*\+\s*', etykieta_wiersza, flags=re.IGNORECASE)
                lista_aktualnych_pojazdow = [p.strip() for p in lista_aktualnych_pojazdow if p.strip()]
            else:
                aktualny_kontrahent = etykieta_wiersza.strip('"')
            continue
        else:
            continue 

        if 'etykieta_do_uzycia' in locals() and etykieta_do_uzycia:
            if not aktualna_data: continue 
            if not (data_start <= aktualna_data <= data_stop): continue 

            pojazdy_do_zapisu = []
            if lista_aktualnych_pojazdow:
                pojazdy_do_zapisu = lista_aktualnych_pojazdow
            elif etykieta_do_uzycia in ETYKIETY_PRZYCHODOW and aktualny_kontrahent and aktualny_kontrahent != "nan":
                pojazdy_do_zapisu = [aktualny_kontrahent]
            else:
                continue
            
            liczba_pojazdow = len(pojazdy_do_zapisu)
            podz_kwota_brutto = kwota_brutto_do_uzycia / liczba_pojazdow
            podz_kwota_netto = kwota_netto_do_uzycia / liczba_pojazdow
            podz_kwota_org = kwota_org_do_uzycia / liczba_pojazdow
            
            opis_transakcji = etykieta_do_uzycia
            kontrahent_do_zapisu = "Brak Kontrahenta"
            if aktualny_kontrahent and aktualny_kontrahent != "nan":
                opis_transakcji = f"{etykieta_do_uzycia} - {aktualny_kontrahent}"
                kontrahent_do_zapisu = aktualny_kontrahent
            
            for pojazd in pojazdy_do_zapisu:
                typ_transakcji = None
                if etykieta_do_uzycia in ETYKIETY_PRZYCHODOW:
                    typ_transakcji = 'Przychód (Subiekt)'
                elif etykieta_do_uzycia in ETYKIETY_KOSZTOW_INNYCH:
                    typ_transakcji = 'Koszt (Subiekt)'
                if typ_transakcji:
                    wyniki.append({
                        'data': aktualna_data, 
                        'pojazd_oryg': pojazd, 
                        'opis': opis_transakcji,
                        'typ': typ_transakcji, 
                        'zrodlo': 'Subiekt',
                        'kwota_brutto_eur': podz_kwota_brutto,
                        'kwota_netto_eur': podz_kwota_netto,
                        'kontrahent': kontrahent_do_zapisu,
                        'kwota_org': podz_kwota_org,
                        'waluta_org': waluta_do_uzycia
                    })
            del etykieta_do_uzycia
            kwota_brutto_do_uzycia = 0.0
            kwota_netto_do_uzycia = 0.0
            waluta_do_uzycia = "EUR"
            kwota_org_do_uzycia = 0.0
            
    if not wyniki:
        return None, None 

    df_wyniki = pd.DataFrame(wyniki)
    maska_zakazana = df_wyniki['pojazd_oryg'].apply(czy_zakazany_pojazd_global)
    df_wyniki = df_wyniki[~maska_zakazana]
    
    CZARNA_LISTA_FINALNA = ['TRUCK24SP', 'EDENRED', 'MARMAR', 'INTERCARS', 'SANTANDER', 'LEASING']
    for smiec in CZARNA_LISTA_FINALNA:
        maska = df_wyniki['pojazd_oryg'].astype(str).str.upper().str.contains(smiec, na=False)
        df_wyniki = df_wyniki[~maska]
        
    if df_wyniki.empty:
         return None, None

    df_wyniki['pojazd_clean'] = bezpieczne_czyszczenie_klucza(df_wyniki['pojazd_oryg'])
    maska_brak = df_wyniki['pojazd_clean'] == 'Brak Identyfikatora'
    df_wyniki.loc[maska_brak, 'pojazd_clean'] = df_wyniki.loc[maska_brak, 'pojazd_oryg']
    
    df_wyniki = df_wyniki[~df_wyniki['pojazd_clean'].str.contains('ZALICZKA', case=False, na=False)]

    def zaawansowane_czyszczenie_korekt(df):
        if df.empty: return df
        try:
            df['temp_month'] = pd.to_datetime(df['data']).dt.to_period('M')
        except:
            return df 
        indices_to_drop = []
        grouped = df.groupby(['pojazd_clean', 'kontrahent', 'temp_month'])
        for name, group in grouped:
            opisy_w_grupie = [str(x) for x in group['opis'].unique()]
            ma_korekte_zak = any('Korekta faktury VAT zakupu' in op for op in opisy_w_grupie)
            if ma_korekte_zak:
                for idx, row in group.iterrows():
                    opis_wiersza = str(row['opis'])
                    if 'Korekta faktury VAT zakupu' in opis_wiersza:
                        continue
                    if ('Faktura VAT zakupu' in opis_wiersza or 
                        'Serwis' in opis_wiersza or 
                        'Rachunek zakupu' in opis_wiersza):
                        indices_to_drop.append(idx)
            ma_korekte_sprz = any('Korekta faktury VAT sprzedaży' in op for op in opisy_w_grupie)
            if ma_korekte_sprz:
                 for idx, row in group.iterrows():
                    opis_wiersza = str(row['opis'])
                    if 'Korekta faktury VAT sprzedaży' in opis_wiersza:
                        continue
                    if 'Faktura VAT sprzedaży' in opis_wiersza or 'Rachunek sprzedaży' in opis_wiersza:
                        indices_to_drop.append(idx)
        df_clean = df.drop(indices_to_drop)
        if 'temp_month' in df_clean.columns:
            return df_clean.drop(columns=['temp_month'])
        return df_clean

    df_wyniki = zaawansowane_czyszczenie_korekt(df_wyniki)
    df_przychody = df_wyniki[df_wyniki['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kwota_brutto_eur'].sum().to_frame('przychody_brutto')
    df_przychody_netto = df_wyniki[df_wyniki['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kwota_netto_eur'].sum().to_frame('przychody_netto')
    df_koszty = df_wyniki[df_wyniki['typ'] == 'Koszt (Subiekt)'].groupby('pojazd_clean')['kwota_brutto_eur'].sum().to_frame('koszty_inne_brutto')
    df_koszty_netto = df_wyniki[df_wyniki['typ'] == 'Koszt (Subiekt)'].groupby('pojazd_clean')['kwota_netto_eur'].sum().to_frame('koszty_inne_netto')
    df_agregacja = pd.concat([df_przychody, df_przychody_netto, df_koszty, df_koszty_netto], axis=1).fillna(0)
    st.success(f"Plik analizy przetworzony pomyślnie. Znaleziono {len(df_wyniki)} wpisów.")
    return df_agregacja, df_wyniki

@st.cache_data
def to_excel_extended(df_summary, df_subiekt_raw, df_fuel_raw):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        summary_to_write = df_summary.reset_index().rename(columns={'index': 'Pojazd'})
        summary_to_write.insert(0, 'Lp.', range(1, 1 + len(summary_to_write)))
        if 'Główny Kontrahent' in summary_to_write.columns:
            cols = list(summary_to_write.columns)
            cols.remove('Główny Kontrahent')
            cols.insert(2, 'Główny Kontrahent')
            summary_to_write = summary_to_write[cols]
        summary_to_write.to_excel(writer, sheet_name='Podsumowanie', index=False)
        
        pojazdy_subiekt = set()
        if df_subiekt_raw is not None and not df_subiekt_raw.empty:
            pojazdy_subiekt = set(df_subiekt_raw['pojazd_clean'].unique())
        pojazdy_paliwo = set()
        if df_fuel_raw is not None and not df_fuel_raw.empty:
            pojazdy_paliwo = set(df_fuel_raw['identyfikator_clean'].unique())
        wszystkie_pojazdy = sorted(list(pojazdy_subiekt.union(pojazdy_paliwo)))
        wszystkie_pojazdy = [p for p in wszystkie_pojazdy if not czy_zakazany_pojazd_global(p)]
        
        for pojazd in wszystkie_pojazdy:
            safe_name = re.sub(r'[\\/*?:\[\]]', '', str(pojazd))[:30]
            if not safe_name: safe_name = "Unknown"
            dfs_to_concat = []
            if df_subiekt_raw is not None and not df_subiekt_raw.empty:
                sub_data = df_subiekt_raw[df_subiekt_raw['pojazd_clean'] == pojazd].copy()
                if not sub_data.empty:
                    kwota_org_col = sub_data.get('kwota_org', sub_data['kwota_brutto_eur'])
                    waluta_org_col = sub_data.get('waluta_org', 'EUR')
                    sub_formatted = pd.DataFrame({
                        'Data': sub_data['data'],
                        'Rodzaj': sub_data['typ'],
                        'Opis': sub_data['opis'],
                        'Kwota Oryginalna': kwota_org_col,
                        'Waluta': waluta_org_col,
                        'Kwota EUR (Netto)': sub_data['kwota_netto_eur'],
                        'Kwota EUR (Brutto)': sub_data['kwota_brutto_eur']
                    })
                    sub_formatted.loc[sub_formatted['Rodzaj'] == 'Koszt (Subiekt)', ['Kwota Oryginalna', 'Kwota EUR (Netto)', 'Kwota EUR (Brutto)']] *= -1
                    dfs_to_concat.append(sub_formatted)
            if df_fuel_raw is not None and not df_fuel_raw.empty:
                fuel_data = df_fuel_raw[df_fuel_raw['identyfikator_clean'] == pojazd].copy()
                if not fuel_data.empty:
                    fuel_formatted = pd.DataFrame({
                        'Data': fuel_data['data_transakcji_dt'].dt.date,
                        'Rodzaj': fuel_data['typ'],
                        'Opis': fuel_data['produkt'],
                        'Kwota Oryginalna': -fuel_data['kwota_brutto_num'].abs(),
                        'Waluta': fuel_data['waluta'],
                        'Kwota EUR (Netto)': -fuel_data['kwota_netto_eur'].abs(),
                        'Kwota EUR (Brutto)': -fuel_data['kwota_brutto_eur'].abs()
                    })
                    dfs_to_concat.append(fuel_formatted)
            if dfs_to_concat:
                final_df = pd.concat(dfs_to_concat).sort_values(by='Data')
                final_df.to_excel(writer, sheet_name=safe_name, index=False)
    return output.getvalue()

@st.cache_data
def to_excel_contractors(df_analiza_raw):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df_revenues = df_analiza_raw[df_analiza_raw['typ'] == 'Przychód (Subiekt)'].copy()
        if df_revenues.empty:
            return output.getvalue()
        summary = df_revenues.groupby('kontrahent')[['kwota_netto_eur', 'kwota_brutto_eur']].sum().sort_values(by='kwota_brutto_eur', ascending=False)
        summary = summary.reset_index()
        summary.insert(0, 'Lp.', range(1, 1 + len(summary)))
        summary.to_excel(writer, sheet_name='Podsumowanie', index=False)
        unique_contractors = sorted(df_revenues['kontrahent'].unique())
        for kontrahent in unique_contractors:
            safe_name = re.sub(r'[\\/*?:\[\]]', '', str(kontrahent))[:30]
            if not safe_name: safe_name = "Unknown"
            sub_data = df_revenues[df_revenues['kontrahent'] == kontrahent].copy()
            formatted = pd.DataFrame({
                'Data': sub_data['data'],
                'Pojazd': sub_data['pojazd_clean'], 
                'Opis': sub_data['opis'],
                'Kwota Oryginalna': sub_data.get('kwota_org', 0.0), 
                'Waluta': sub_data.get('waluta_org', 'EUR'),
                'Kwota EUR (Netto)': sub_data['kwota_netto_eur'],
                'Kwota EUR (Brutto)': sub_data['kwota_brutto_eur']
            }).sort_values(by='Data')
            formatted.to_excel(writer, sheet_name=safe_name, index=False)
    return output.getvalue()

# --- HELPERY DO RENDEROWANIA ZAWARTOŚCI ZAKŁADEK (WYCIĄGNIĘTE Z MAIN) ---

def render_admin_content(conn, wybrana_firma):
    st.subheader("Zarządzanie Danymi")
    
    col_up1, col_up2 = st.columns([1, 2])
    
    with col_up1:
        st.info("Wybierz pliki z dysku (Excel/CSV), a następnie przypisz je do odpowiedniej firmy i wgraj do bazy.")
        firma_upload = st.selectbox("Przypisz plik do firmy:", FIRMY, index=FIRMY.index(wybrana_firma))
        
    with col_up2:
        with st.container(border=True):
            przeslane_pliki = st.file_uploader(
                "Wybierz pliki (Eurowag, E100, Fakturownia)",
                accept_multiple_files=True,
                type=['xlsx', 'xls', 'csv']
            )
            if przeslane_pliki:
                if st.button("🚀 Przetwórz i wgraj do bazy", type="primary", use_container_width=True):
                    with st.spinner("Wczytywanie i unifikowanie plików..."):
                        dane_do_wgrania, blad = wczytaj_i_zunifikuj_pliki(przeslane_pliki, firma_upload)
                    if blad:
                        st.error(blad)
                    elif dane_do_wgrania is None or dane_do_wgrania.empty:
                        st.error("Nie udało się przetworzyć żadnych danych. Sprawdź pliki.")
                    else:
                        st.success(f"Zunifikowano {len(dane_do_wgrania)} nowych transakcji dla firmy {firma_upload}.")
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
                                st.stop()
                        st.success("Dane zostały pomyślnie zapisane w bazie!")
                        with st.spinner("Czyszczenie duplikatów..."):
                            wyczysc_duplikaty(conn)
                        st.success("Baza danych została oczyszczona.")

    st.markdown("---")
    with st.expander("⚠️ Strefa Niebezpieczna (Reset Bazy)", expanded=False):
        st.warning("Poniższe operacje usuwają dane! Używaj ostrożnie.")
        col_admin_l, col_admin_r = st.columns(2)
        with col_admin_l:
            if st.button("🗑️ 1. Wyczyść transakcje (Tabela Transactions)"):
                with st.spinner("Resetowanie tabeli transakcji..."):
                    setup_database(conn)
                st.success("Tabela transakcji została wyczyszczona.")
        with col_admin_r:
            if st.button("🗑️ 2. Wyczyść pliki (Tabela Saved_Files)"):
                with st.spinner("Resetowanie tabeli plików..."):
                    setup_file_database(conn)
                st.success("Tabela plików została wyczyszczona.")

def render_raport_content(conn, wybrana_firma):
    st.subheader("Raport Paliw i Opłat")
    if wybrana_firma == "UNIX-TRANS":
        st.caption("ℹ️ Wyświetlam wydatki UNIX-TRANS (bez pojazdów obcych).")
    
    try:
        min_max_date_query = f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}"
        min_max_date = conn.query(min_max_date_query)
        
        if min_max_date.empty or min_max_date.iloc[0, 0] is None:
            st.info("Baza danych jest pusta. Wgraj pliki w Panelu Admina.")
        else:
            domyslny_start = min_max_date.iloc[0, 0]
            domyslny_stop = min_max_date.iloc[0, 1]
            
            with st.container(border=True):
                st.markdown("##### 📅 Zakres Raportu")
                col_r1, col_r2 = st.columns(2)
                with col_r1:
                    data_start_rap = st.date_input("Data Start", value=domyslny_start, min_value=domyslny_start, max_value=domyslny_stop, key="rap_start")
                with col_r2:
                    data_stop_rap = st.date_input("Data Stop", value=domyslny_stop, min_value=domyslny_start, max_value=domyslny_stop, key="rap_stop")

            dane_z_bazy_full = pobierz_dane_z_bazy(conn, data_start_rap, data_stop_rap, wybrana_firma)
            
            if dane_z_bazy_full.empty:
                st.warning(f"Brak danych dla firmy {wybrana_firma} w wybranym zakresie dat.")
            else:
                dane_przygotowane, mapa_kursow = przygotuj_dane_paliwowe(dane_z_bazy_full.copy(), wybrana_firma)
                
                if dane_przygotowane is None: st.stop()
                
                sub_tab_paliwo, sub_tab_oplaty, sub_tab_inne = st.tabs(["⛽ Paliwo", "🛣️ Opłaty Drogowe", "🛒 Pozostałe"])
                
                with sub_tab_paliwo:
                    df_paliwo = dane_przygotowane[dane_przygotowane['typ'] == 'PALIWO']
                    if df_paliwo.empty:
                        st.info("Brak danych o paliwie.")
                    else:
                        st.metric(label="Łącznie Paliwo (Brutto)", value=f"{df_paliwo['kwota_brutto_eur'].sum():,.2f} EUR", border=True)
                        
                        st.markdown("##### 🗺️ Wydatki paliwowe wg Kraju")
                        if 'kraj' in df_paliwo.columns:
                            df_kraje = df_paliwo.groupby('kraj').agg(
                                Suma_Netto=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                                Suma_Brutto=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                            ).sort_values(by='Suma_Brutto', ascending=False)
                            df_kraje['VAT'] = df_kraje['Suma_Brutto'] - df_kraje['Suma_Netto']
                            df_kraje = df_kraje[['Suma_Netto', 'VAT', 'Suma_Brutto']]
                            st.bar_chart(df_kraje['Suma_Brutto'], color="#FF4B4B")
                            
                            # --- MODYFIKACJA LP ---
                            df_kraje_show = df_kraje.reset_index()
                            df_kraje_show.insert(0, 'Lp.', range(1, 1 + len(df_kraje_show)))
                            st.dataframe(df_kraje_show.style.format("{:,.2f} EUR", subset=['Suma_Netto', 'VAT', 'Suma_Brutto']), use_container_width=True, hide_index=True)
                        
                        st.markdown("##### 🚛 Szczegóły per Pojazd")
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
                        
                        # --- MODYFIKACJA LP ---
                        df_podsumowanie_show = podsumowanie_paliwo[['Kwota_Netto_EUR', 'Kwota_Brutto_EUR', 'Litry (Diesel)', 'Litry (AdBlue)']].reset_index()
                        df_podsumowanie_show.insert(0, 'Lp.', range(1, 1 + len(df_podsumowanie_show)))

                        st.dataframe(df_podsumowanie_show.style.format({'Kwota_Netto_EUR': '{:,.2f} EUR', 'Kwota_Brutto_EUR': '{:,.2f} EUR', 'Litry (Diesel)': '{:,.2f} L', 'Litry (AdBlue)': '{:,.2f} L'}), use_container_width=True, hide_index=True)

                        with st.expander("🔎 Pokaż pojedyncze transakcje"):
                            lista_pojazdow_paliwo = ["--- Wybierz pojazd ---"] + sorted(list(df_paliwo['identyfikator_clean'].unique()))
                            wybrany_pojazd_paliwo = st.selectbox("Wybierz identyfikator:", lista_pojazdow_paliwo)
                            if wybrany_pojazd_paliwo != "--- Wybierz pojazd ---":
                                df_szczegoly = df_paliwo[df_paliwo['identyfikator_clean'] == wybrany_pojazd_paliwo].sort_values(by='data_transakcji_dt', ascending=False)
                                df_szczegoly_display = df_szczegoly[['data_transakcji_dt', 'produkt', 'kraj', 'ilosc', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                
                                # --- MODYFIKACJA LP ---
                                df_szczegoly_display = df_szczegoly_display.rename(columns={'data_transakcji_dt': 'Data', 'produkt': 'Produkt', 'kraj': 'Kraj', 'ilosc': 'Litry', 'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'})
                                df_szczegoly_display.insert(0, 'Lp.', range(1, 1 + len(df_szczegoly_display)))
                                
                                st.dataframe(
                                    df_szczegoly_display,
                                    use_container_width=True, hide_index=True,
                                    column_config={"Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"), "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"), "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"), "Litry": st.column_config.NumberColumn(format="%.2f L"),}
                                )
                
                with sub_tab_oplaty:
                      df_oplaty = dane_przygotowane[dane_przygotowane['typ'] == 'OPŁATA']
                      if not df_oplaty.empty:
                          st.metric(label="Łącznie Opłaty (Brutto)", value=f"{df_oplaty['kwota_brutto_eur'].sum():,.2f} EUR", border=True)

                          podsumowanie_oplaty = df_oplaty.groupby('identyfikator_clean').agg(
                              Kwota_Netto_EUR=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                              Kwota_Brutto_EUR=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                          ).sort_values(by='Kwota_Brutto_EUR', ascending=False)
                          
                          # --- MODYFIKACJA LP ---
                          df_oplaty_show = podsumowanie_oplaty.reset_index()
                          df_oplaty_show.insert(0, 'Lp.', range(1, 1 + len(df_oplaty_show)))
                          st.dataframe(df_oplaty_show.style.format("{:,.2f} EUR", subset=['Kwota_Netto_EUR', 'Kwota_Brutto_EUR']), use_container_width=True, hide_index=True)
                          
                          with st.expander("🔎 Pokaż pojedyncze transakcje"):
                              lista_pojazdow_oplaty = ["--- Wybierz pojazd ---"] + sorted(list(df_oplaty['identyfikator_clean'].unique()))
                              wybrany_pojazd_oplaty = st.selectbox("Wybierz identyfikator:", lista_pojazdow_oplaty, key="select_oplaty")
                              if wybrany_pojazd_oplaty != "--- Wybierz pojazd ---":
                                 df_szczegoly_oplaty = df_oplaty[df_oplaty['identyfikator_clean'] == wybrany_pojazd_oplaty].sort_values(by='data_transakcji_dt', ascending=False)
                                 df_szczegoly_oplaty_display = df_szczegoly_oplaty[['data_transakcji_dt', 'produkt', 'kraj', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                 
                                 # --- MODYFIKACJA LP ---
                                 df_szczegoly_oplaty_display = df_szczegoly_oplaty_display.rename(columns={'data_transakcji_dt': 'Data', 'produkt': 'Opis', 'kraj': 'Kraj', 'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'})
                                 df_szczegoly_oplaty_display.insert(0, 'Lp.', range(1, 1 + len(df_szczegoly_oplaty_display)))

                                 st.dataframe(
                                     df_szczegoly_oplaty_display,
                                     use_container_width=True, hide_index=True,
                                     column_config={"Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"), "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"), "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),}
                                 )
                      else:
                          st.info("Brak opłat drogowych.")

                with sub_tab_inne:
                      df_inne = dane_przygotowane[dane_przygotowane['typ'] == 'INNE']
                      if not df_inne.empty:
                          st.metric(label="Łącznie Inne (Brutto)", value=f"{df_inne['kwota_brutto_eur'].sum():,.2f} EUR", border=True)
                          
                          podsumowanie_inne = df_inne.groupby('identyfikator_clean').agg(
                              Kwota_Netto_EUR=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                              Kwota_Brutto_EUR=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                          ).sort_values(by='Kwota_Brutto_EUR', ascending=False)
                          
                          # --- MODYFIKACJA LP ---
                          df_inne_show = podsumowanie_inne.reset_index()
                          df_inne_show.insert(0, 'Lp.', range(1, 1 + len(df_inne_show)))
                          st.dataframe(df_inne_show.style.format("{:,.2f} EUR", subset=['Kwota_Netto_EUR', 'Kwota_Brutto_EUR']), use_container_width=True, hide_index=True)

                          with st.expander("🔎 Pokaż pojedyncze transakcje"):
                              lista_pojazdow_inne = ["--- Wybierz pojazd ---"] + sorted(list(df_inne['identyfikator_clean'].unique()))
                              wybrany_pojazd_inne = st.selectbox("Wybierz identyfikator:", lista_pojazdow_inne, key="select_inne")
                              if wybrany_pojazd_inne != "--- Wybierz pojazd ---":
                                 df_szczegoly_inne = df_inne[df_inne['identyfikator_clean'] == wybrany_pojazd_inne].sort_values(by='data_transakcji_dt', ascending=False)
                                 df_szczegoly_inne_display = df_szczegoly_inne[['data_transakcji_dt', 'produkt', 'kraj', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                 
                                 # --- MODYFIKACJA LP ---
                                 df_szczegoly_inne_display = df_szczegoly_inne_display.rename(columns={'data_transakcji_dt': 'Data', 'produkt': 'Opis', 'kraj': 'Kraj', 'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'})
                                 df_szczegoly_inne_display.insert(0, 'Lp.', range(1, 1 + len(df_szczegoly_inne_display)))

                                 st.dataframe(
                                     df_szczegoly_inne_display,
                                     use_container_width=True, hide_index=True,
                                     column_config={"Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"), "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"), "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),}
                                 )
                      else:
                          st.info("Brak innych wydatków.")
    except Exception as e:
        if "does not exist" in str(e):
             st.warning("Baza danych nie jest gotowa. Przejdź do Panelu Admina.")
        else:
             st.error(f"Błąd: {e}")
def render_rentownosc_content(conn, wybrana_firma):
    st.subheader("Analiza Rentowności")
    try:
        min_max_date_query = f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}"
        min_max_date = conn.query(min_max_date_query)
        domyslny_start_rent = date.today()
        domyslny_stop_rent = date.today()
        if not min_max_date.empty and min_max_date.iloc[0, 0] is not None:
            domyslny_start_rent = min_max_date.iloc[0, 0]
            domyslny_stop_rent = min_max_date.iloc[0, 1]
        
        col_rent_settings, col_rent_action = st.columns([1, 2])
        
        with col_rent_settings:
            with st.container(border=True):
                st.markdown("##### 1. Zakres Dat")
                data_start_rent = st.date_input("Start", value=domyslny_start_rent, key="rent_start_2")
                data_stop_rent = st.date_input("Stop", value=domyslny_stop_rent, key="rent_stop_2")
            
            with st.container(border=True):
                st.markdown("##### 2. Źródło Przychodów")
                plik_analizy = None 
                nazwa_pliku_analizy = "analiza.xlsx"
                if wybrana_firma == "UNIX-TRANS":
                    nazwa_pliku_analizy = "fakturownia.csv"
                    st.caption("Wymagany: Fakturownia CSV")
                else:
                    st.caption("Wymagany: Subiekt Excel")
                    
                zapisany_plik_bytes = wczytaj_plik_z_bazy(conn, nazwa_pliku_analizy) 
                
                if zapisany_plik_bytes:
                      st.success(f"Znaleziono w bazie: {nazwa_pliku_analizy}")
                      plik_analizy = io.BytesIO(zapisany_plik_bytes)
                      if st.button("❌ Usuń zapisany plik", use_container_width=True):
                          usun_plik_z_bazy(conn, nazwa_pliku_analizy)
                else:
                    uploaded_file = st.file_uploader(f"Wgraj {nazwa_pliku_analizy}", type=['xlsx', 'csv', 'xls'])
                    if uploaded_file:
                         plik_analizy = uploaded_file
                         if st.button("💾 Zapisz plik na stałe w bazie", use_container_width=True):
                             zapisz_plik_w_bazie(conn, nazwa_pliku_analizy, uploaded_file.getvalue())

        with col_rent_action:
            st.info("Kliknij przycisk poniżej, aby połączyć dane o przychodach z wydatkami paliwowymi i obliczyć wynik.")
            if 'raport_gotowy' not in st.session_state:
                st.session_state['raport_gotowy'] = False
            if 'wybrany_pojazd_rent' not in st.session_state:
                st.session_state['wybrany_pojazd_rent'] = "--- Wybierz pojazd ---"

            if st.button("🚀 Generuj Raport Rentowności", type="primary", use_container_width=True):
                if plik_analizy is None:
                    st.error("Brak pliku przychodów.")
                else:
                    with st.spinner("Przeliczanie rentowności..."):
                        dane_z_bazy_rent = pobierz_dane_z_bazy(conn, data_start_rent, data_stop_rent, wybrana_firma) 
                        dane_przygotowane_rent, _ = przygotuj_dane_paliwowe(dane_z_bazy_rent.copy(), wybrana_firma)
                        st.session_state['dane_bazy_raw'] = dane_przygotowane_rent 
                        if dane_przygotowane_rent.empty:
                            df_koszty_baza_agg = pd.DataFrame(columns=['koszty_baza_netto', 'koszty_baza_brutto'])
                        else:
                            maska_baza = dane_przygotowane_rent['identyfikator_clean'].apply(czy_zakazany_pojazd_global)
                            dane_przygotowane_rent = dane_przygotowane_rent[~maska_baza]
                            df_koszty_baza_agg = dane_przygotowane_rent.groupby('identyfikator_clean').agg(
                                koszty_baza_netto=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                                koszty_baza_brutto=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                            )
                        df_analiza_agreg, df_analiza_raw = przetworz_plik_analizy(plik_analizy, data_start_rent, data_stop_rent, wybrana_firma)
                        st.session_state['dane_analizy_raw'] = df_analiza_raw 
                        if df_analiza_agreg is None:
                             df_analiza_agreg = pd.DataFrame(columns=['przychody_brutto', 'przychody_netto', 'koszty_inne_brutto', 'koszty_inne_netto'])
                        
                        df_rentownosc = df_analiza_agreg.merge(
                            df_koszty_baza_agg, 
                            left_index=True, 
                            right_index=True, 
                            how='outer'
                        ).fillna(0)
                        
                        # --- FILTRY SPECJALNE (PTU0002 i NONE) ---
                        maska_ptu_usun = df_rentownosc.index.astype(str).str.replace(" ", "").str.replace("-", "").str.contains("PTU0002")
                        df_rentownosc = df_rentownosc[~maska_ptu_usun]
                        
                        maska_none_usun = df_rentownosc.index.astype(str).str.upper() == "NONE"
                        df_rentownosc = df_rentownosc[~maska_none_usun]
                        # -----------------------------------------

                        maska_index = df_rentownosc.index.to_series().apply(czy_zakazany_pojazd_global)
                        df_rentownosc = df_rentownosc[~maska_index]
                        
                        if df_analiza_raw is not None and not df_analiza_raw.empty:
                             def zlacz_kontrahentow(x):
                                   unikalne = sorted(list(set([k for k in x if k and k != "Brak Kontrahenta"])))
                                   if not unikalne: return "Brak danych"
                                   return ", ".join(unikalne)
                             df_kontrahenci_mapa = df_analiza_raw[df_analiza_raw['typ'] == 'Przychód (Subiekt)'].groupby('pojazd_clean')['kontrahent'].apply(zlacz_kontrahentow).to_frame('Główny Kontrahent')
                             df_rentownosc = df_rentownosc.merge(df_kontrahenci_mapa, left_index=True, right_index=True, how='left').fillna('Brak danych')
                        
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
                        st.session_state['df_rentownosc'] = df_rentownosc
                        st.session_state['raport_gotowy'] = True
                        st.session_state['wybrany_pojazd_rent'] = "--- Wybierz pojazd ---"

        # --- SEKCJA WYNIKÓW ---
        if st.session_state.get('raport_gotowy', False):
             st.markdown("---")
             df_rentownosc = st.session_state['df_rentownosc']
             df_analiza_raw = st.session_state.get('dane_analizy_raw')

             if df_analiza_raw is not None and not df_analiza_raw.empty:
                    maska_raw = df_analiza_raw['pojazd_clean'].apply(czy_zakazany_pojazd_global)
                    df_analiza_raw = df_analiza_raw[~maska_raw]

             if df_analiza_raw is not None and not df_analiza_raw.empty:
                    tab_chart_kontrahent, tab_chart_pojazd = st.tabs(["🏢 Wykres: Kontrahenci", "🚛 Wykres: Pojazdy"])
                    with tab_chart_kontrahent:
                        df_chart_kontr = df_analiza_raw[df_analiza_raw['typ'] == 'Przychód (Subiekt)'].copy()
                        if not df_chart_kontr.empty:
                            chart_data = df_chart_kontr.groupby('kontrahent')['kwota_brutto_eur'].sum().sort_values(ascending=False)
                            st.bar_chart(chart_data)
                            
                            with st.expander("Szczegóły wg Kontrahenta"):
                                lista_kontrahentow = sorted(df_chart_kontr['kontrahent'].unique().tolist())
                                wybrany_kontrahent_view = st.multiselect("Filtruj tabelę:", lista_kontrahentow)
                                excel_contractors = to_excel_contractors(df_analiza_raw)
                                st.download_button(
                                    label="📥 Pobierz Excel (Wg Kontrahentów)",
                                    data=excel_contractors,
                                    file_name=f"raport_kontrahenci_{data_start_rent}.xlsx",
                                    mime="application/vnd.ms-excel"
                                )
                                if wybrany_kontrahent_view:
                                    df_show = df_chart_kontr[df_chart_kontr['kontrahent'].isin(wybrany_kontrahent_view)]
                                    
                                    # --- MODYFIKACJA LP ---
                                    df_show_display = df_show[['data', 'pojazd_clean', 'opis', 'kwota_netto_eur', 'kwota_brutto_eur']].copy()
                                    df_show_display.insert(0, 'Lp.', range(1, 1 + len(df_show_display)))

                                    st.dataframe(
                                        df_show_display.style.format({'kwota_netto_eur': '{:,.2f} EUR', 'kwota_brutto_eur': '{:,.2f} EUR'}), 
                                        use_container_width=True, 
                                        hide_index=True
                                    )
                    with tab_chart_pojazd:
                        df_chart_poj = df_analiza_raw[df_analiza_raw['typ'] == 'Przychód (Subiekt)'].copy()
                        if not df_chart_poj.empty:
                            chart_data_poj = df_chart_poj.groupby('pojazd_clean')['kwota_brutto_eur'].sum().sort_values(ascending=False)
                            st.bar_chart(chart_data_poj, color="#76b900")
             
             st.subheader("Wyniki Finansowe")
             st.metric("SUMA ZYSK (BRUTTO)", f"{df_rentownosc['ZYSK_STRATA_BRUTTO_EUR'].sum():,.2f} EUR", border=True)

             cols_show = [
                'przychody_netto', 'przychody_brutto', 
                'koszty_inne_netto', 'koszty_inne_brutto',
                'koszty_baza_netto', 'koszty_baza_brutto',
                'ZYSK_STRATA_NETTO_EUR', 'ZYSK_STRATA_BRUTTO_EUR'
             ]
             if 'Główny Kontrahent' in df_rentownosc.columns:
                 cols_show.insert(0, 'Główny Kontrahent')
             
             # --- MODYFIKACJA LP ---
             df_rent_show = df_rentownosc[cols_show].reset_index().rename(columns={'index': 'Pojazd'})
             df_rent_show.insert(0, 'Lp.', range(1, 1 + len(df_rent_show)))

             st.dataframe(df_rent_show.style.format("{:,.2f} EUR", subset=['przychody_netto', 'przychody_brutto', 'koszty_inne_netto', 'koszty_inne_brutto', 'koszty_baza_netto', 'koszty_baza_brutto', 'ZYSK_STRATA_NETTO_EUR', 'ZYSK_STRATA_BRUTTO_EUR']), use_container_width=True, hide_index=True)
             
             dane_bazy_raw_export = st.session_state.get('dane_bazy_raw')
             df_analiza_raw = st.session_state.get('dane_analizy_raw')
             excel_data = to_excel_extended(df_rentownosc, df_analiza_raw, dane_bazy_raw_export)
             st.download_button(
                label="📥 Pobierz PEŁNY Raport Rentowności (Excel)",
                data=excel_data,
                file_name=f"raport_{wybrana_firma}_{data_start_rent}.xlsx",
                mime="application/vnd.ms-excel",
                type="primary"
             )

             st.markdown("---")
             st.markdown("##### 🕵️ Analiza szczegółowa pojazdu")
             df_rentownosc_sorted = df_rentownosc.sort_values(by='ZYSK_STRATA_BRUTTO_EUR', ascending=False)
             lista_pojazdow_rent = ["--- Wybierz pojazd ---"] + list(df_rentownosc_sorted.index.unique())
             wybrany_pojazd_rent = st.selectbox("Wybierz pojazd:", lista_pojazdow_rent, key='wybrany_pojazd_rent')
             
             if wybrany_pojazd_rent != "--- Wybierz pojazd ---":
                  try:
                    dane_pojazdu = df_rentownosc_sorted.loc[wybrany_pojazd_rent]
                    przychody = dane_pojazdu['przychody_brutto']
                    koszty_inne = dane_pojazdu['koszty_inne_brutto']
                    koszty_bazy = dane_pojazdu['koszty_baza_brutto']
                    zysk = dane_pojazdu['ZYSK_STRATA_BRUTTO_EUR']
                    delta_color = "normal" if zysk >= 0 else "inverse"
                    
                    col_det1, col_det2, col_det3, col_det4 = st.columns(4)
                    col_det1.metric("Zysk/Strata", f"{zysk:,.2f} EUR", delta_color=delta_color)
                    col_det2.metric("Przychód", f"{przychody:,.2f} EUR")
                    col_det3.metric("Koszty Inne", f"{-koszty_inne:,.2f} EUR")
                    col_det4.metric("Paliwo/Opłaty", f"{-koszty_bazy:,.2f} EUR")
                    
                    st.caption(f"Szczegóły transakcji: {wybrany_pojazd_rent}")
                    dane_przygotowane_rent = st.session_state.get('dane_bazy_raw')
                    lista_df_szczegolow = []
                    
                    if df_analiza_raw is not None and not df_analiza_raw.empty:
                        subiekt_details = df_analiza_raw[df_analiza_raw['pojazd_clean'] == wybrany_pojazd_rent].copy()
                        if not subiekt_details.empty:
                            subiekt_formatted = subiekt_details.copy()
                            mask_koszt = subiekt_formatted['typ'] == 'Koszt (Subiekt)'
                            subiekt_formatted.loc[mask_koszt, 'kwota_netto_eur'] = -subiekt_formatted.loc[mask_koszt, 'kwota_netto_eur'].abs()
                            subiekt_formatted.loc[mask_koszt, 'kwota_brutto_eur'] = -subiekt_formatted.loc[mask_koszt, 'kwota_brutto_eur'].abs()
                            subiekt_formatted = subiekt_formatted[['data', 'opis', 'zrodlo', 'kwota_netto_eur', 'kwota_brutto_eur']]
                            lista_df_szczegolow.append(subiekt_formatted)
                    
                    if dane_przygotowane_rent is not None and not dane_przygotowane_rent.empty:
                        baza_details = dane_przygotowane_rent[dane_przygotowane_rent['identyfikator_clean'] == wybrany_pojazd_rent].copy()
                        if not baza_details.empty:
                            baza_formatted = baza_details[['data_transakcji_dt', 'produkt', 'zrodlo', 'kwota_netto_eur', 'kwota_brutto_eur']].copy() 
                            baza_formatted['data_transakcji_dt'] = baza_formatted['data_transakcji_dt'].dt.date
                            baza_formatted.rename(columns={'data_transakcji_dt': 'data', 'produkt': 'opis'}, inplace=True)
                            baza_formatted['kwota_netto_eur'] = -baza_formatted['kwota_netto_eur'].abs() 
                            baza_formatted['kwota_brutto_eur'] = -baza_formatted['kwota_brutto_eur'].abs() 
                            lista_df_szczegolow.append(baza_formatted[['data', 'opis', 'zrodlo', 'kwota_netto_eur', 'kwota_brutto_eur']])
                    
                    if lista_df_szczegolow:
                        combined_details = pd.concat(lista_df_szczegolow).sort_values(by='data', ascending=False)
                        def koloruj_kwoty(val):
                            if pd.isna(val): return ''
                            color = 'red' if val < 0 else 'green'
                            return f'color: {color}'
                        
                        # --- MODYFIKACJA LP ---
                        combined_details.insert(0, 'Lp.', range(1, 1 + len(combined_details)))

                        st.dataframe(combined_details.style.apply(axis=1, subset=['kwota_brutto_eur'], func=lambda row: [koloruj_kwoty(row.kwota_brutto_eur)]), use_container_width=True, hide_index=True, column_config={"data": st.column_config.DateColumn("Data"), "kwota_brutto_eur": st.column_config.NumberColumn("Brutto (EUR)", format="%.2f EUR"), "kwota_netto_eur": st.column_config.NumberColumn("Netto (EUR)", format="%.2f EUR")})
                    else:
                        st.info("Brak szczegółów.")
                  except KeyError:
                    st.error("Błąd wyświetlania szczegółów.")
    except Exception as e:
        st.error(f"Błąd: {e}")
        
def render_refaktury_content(conn, wybrana_firma):
    st.subheader("🔄 Refaktury Kosztów (Wzajemne)")
    st.info("Ta sekcja pokazuje koszty paliwa/opłat poniesione przez jedną firmę na rzecz aut drugiej firmy.")
    
    try:
        min_max_date_query = f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}"
        min_max_date = conn.query(min_max_date_query)
        domyslny_start_ref = date.today()
        domyslny_stop_ref = date.today()
        if not min_max_date.empty and min_max_date.iloc[0, 0] is not None:
            domyslny_start_ref = min_max_date.iloc[0, 0]
            domyslny_stop_ref = min_max_date.iloc[0, 1]
            
        with st.container(border=True):
            c1, c2 = st.columns(2)
            with c1:
                data_start_ref = st.date_input("Data Start", value=domyslny_start_ref, key="ref_start")
            with c2:
                data_stop_ref = st.date_input("Data Stop", value=domyslny_stop_ref, key="ref_stop")
            
        if st.button("🔎 Pokaż koszty do refaktury", type="primary"):
            df_holier_to_unix, df_unix_to_holier, _ = pobierz_dane_do_refaktury(conn, data_start_ref, data_stop_ref)
            
            tab_h2u, tab_u2h = st.tabs(["➡️ Holier -> Unix (Do zwrotu przez Unix)", "⬅️ Unix -> Holier (Do zwrotu przez Holier)"])
            
            # --- TAB 1: HOLIER PLACI ZA UNIX ---
            with tab_h2u:
                st.markdown("### Koszty Holiera na rzecz aut UNIX")
                if df_holier_to_unix is None or df_holier_to_unix.empty:
                    st.success("Brak kosztów w tym kierunku.")
                else:
                    col_met1, col_met2 = st.columns(2)
                    col_met1.metric("Do refaktury (Netto)", f"{df_holier_to_unix['kwota_netto_eur'].sum():,.2f} EUR", border=True)
                    col_met2.metric("Do refaktury (Brutto)", f"{df_holier_to_unix['kwota_brutto_eur'].sum():,.2f} EUR", border=True)
                    
                    st.markdown("##### Podział na pojazdy")
                    agg_ref = df_holier_to_unix.groupby('identyfikator_clean').agg(
                        Suma_Netto=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                        Suma_Brutto=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                    ).sort_values(by='Suma_Brutto', ascending=False)
                    
                    # --- MODYFIKACJA LP ---
                    agg_ref_show = agg_ref.reset_index()
                    agg_ref_show.insert(0, 'Lp.', range(1, 1 + len(agg_ref_show)))
                    st.dataframe(agg_ref_show.style.format("{:,.2f} EUR", subset=['Suma_Netto', 'Suma_Brutto']), use_container_width=True, hide_index=True)
                    
                    with st.expander("Szczegóły transakcji"):
                        # --- MODYFIKACJA LP ---
                        df_ref_show = df_holier_to_unix[['data_transakcji_dt', 'identyfikator_clean', 'produkt', 'kraj', 'kwota_netto_eur', 'kwota_brutto_eur', 'zrodlo']].sort_values(by='data_transakcji_dt', ascending=False).copy()
                        df_ref_show.insert(0, 'Lp.', range(1, 1 + len(df_ref_show)))

                        st.dataframe(
                            df_ref_show,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "data_transakcji_dt": st.column_config.DatetimeColumn("Data", format="YYYY-MM-DD HH:mm"), "kwota_brutto_eur": st.column_config.NumberColumn("Brutto", format="%.2f EUR")
                            }
                        )

            # --- TAB 2: UNIX PLACI ZA HOLIER ---
            with tab_u2h:
                st.markdown("### Koszty UNIX na rzecz aut Holiera (np. NOL0935C)")
                if df_unix_to_holier is None or df_unix_to_holier.empty:
                    st.success("Brak kosztów w tym kierunku.")
                else:
                    col_met1, col_met2 = st.columns(2)
                    col_met1.metric("Do refaktury (Netto)", f"{df_unix_to_holier['kwota_netto_eur'].sum():,.2f} EUR", border=True)
                    col_met2.metric("Do refaktury (Brutto)", f"{df_unix_to_holier['kwota_brutto_eur'].sum():,.2f} EUR", border=True)
                    
                    st.markdown("##### Podział na pojazdy")
                    agg_ref_2 = df_unix_to_holier.groupby('identyfikator_clean').agg(
                        Suma_Netto=pd.NamedAgg(column='kwota_netto_eur', aggfunc='sum'),
                        Suma_Brutto=pd.NamedAgg(column='kwota_brutto_eur', aggfunc='sum')
                    ).sort_values(by='Suma_Brutto', ascending=False)
                    
                    # --- MODYFIKACJA LP ---
                    agg_ref_2_show = agg_ref_2.reset_index()
                    agg_ref_2_show.insert(0, 'Lp.', range(1, 1 + len(agg_ref_2_show)))
                    st.dataframe(agg_ref_2_show.style.format("{:,.2f} EUR", subset=['Suma_Netto', 'Suma_Brutto']), use_container_width=True, hide_index=True)
                    
                    with st.expander("Szczegóły transakcji"):
                        # --- MODYFIKACJA LP ---
                        df_ref_show_2 = df_unix_to_holier[['data_transakcji_dt', 'identyfikator_clean', 'produkt', 'kraj', 'kwota_netto_eur', 'kwota_brutto_eur', 'zrodlo']].sort_values(by='data_transakcji_dt', ascending=False).copy()
                        df_ref_show_2.insert(0, 'Lp.', range(1, 1 + len(df_ref_show_2)))

                        st.dataframe(
                            df_ref_show_2,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "data_transakcji_dt": st.column_config.DatetimeColumn("Data", format="YYYY-MM-DD HH:mm"), "kwota_brutto_eur": st.column_config.NumberColumn("Brutto", format="%.2f EUR")
                            }
                        )
    except Exception as e:
            st.error(f"Błąd: {e}")

        

                
def render_porownanie_content(conn, wybrana_firma):
    st.subheader("📊 Porównanie Okresów")
    st.caption(f"Analiza porównawcza dla firmy: {wybrana_firma}")

    # --- DEFINICJE FUNKCJI KOLORUJĄCYCH ---
    def color_positive_good(val):
        if pd.isna(val) or val == 0: return ''
        color = '#c9f7c9' if val > 0 else '#ffbdbd' 
        return f'background-color: {color}; color: black'

    def color_negative_good(val):
        if pd.isna(val) or val == 0: return ''
        color = '#c9f7c9' if val < 0 else '#ffbdbd' 
        return f'background-color: {color}; color: black'
    # ---------------------------------------

    # --- KONFIGURACJA DAT ---
    today = date.today()
    first_current = today.replace(day=1)
    last_month_end = first_current - pd.Timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    with st.expander("📅 Konfiguracja Okresów", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### Okres A (Bieżący/Bazowy)")
            start_A = st.date_input("Start A", value=first_current, key="start_A")
            stop_A = st.date_input("Stop A", value=today, key="stop_A")
        with c2:
            st.markdown("##### Okres B (Do porównania)")
            start_B = st.date_input("Start B", value=last_month_start, key="start_B")
            stop_B = st.date_input("Stop B", value=last_month_end, key="stop_B")

    # --- OBSŁUGA PLIKU ---
    plik_analizy = None 
    nazwa_pliku_analizy = "analiza.xlsx"
    if wybrana_firma == "UNIX-TRANS":
        nazwa_pliku_analizy = "fakturownia.csv"
    
    zapisany_plik_bytes = wczytaj_plik_z_bazy(conn, nazwa_pliku_analizy)
    if zapisany_plik_bytes:
        plik_analizy = io.BytesIO(zapisany_plik_bytes)
    else:
        st.warning(f"Brak zapisanego pliku {nazwa_pliku_analizy}. Dane z Subiekta/Fakturowni nie będą dostępne.")
        uploaded = st.file_uploader(f"Wgraj tymczasowo {nazwa_pliku_analizy}", type=['xlsx', 'csv', 'xls'], key="por_upload")
        if uploaded:
            plik_analizy = uploaded

    # --- FUNKCJA POBIERANIA DANYCH ---
    def pobierz_agregacje(d_start, d_stop):
        # 1. Dane z Bazy
        df_baza = pobierz_dane_z_bazy(conn, d_start, d_stop, wybrana_firma)
        df_baza, _ = przygotuj_dane_paliwowe(df_baza.copy(), wybrana_firma)
        
        agg_baza = pd.DataFrame()
        if df_baza is not None and not df_baza.empty:
            # Filtrowanie globalne zakazanych
            maska = df_baza['identyfikator_clean'].apply(czy_zakazany_pojazd_global)
            df_baza = df_baza[~maska]

            # --- FILTR USUWAJĄCY PTU0002 ---
            maska_ptu = df_baza['identyfikator_clean'].astype(str).str.replace(" ", "").str.contains("PTU0002", case=False)
            df_baza = df_baza[~maska_ptu]
            
            # --- FILTR USUWAJĄCY NONE ---
            maska_none = df_baza['identyfikator_clean'].astype(str).str.upper() == "NONE"
            df_baza = df_baza[~maska_none]
            # ----------------------------
            
            # Pivot table
            agg_baza = df_baza.groupby(['identyfikator_clean', 'typ'])['kwota_brutto_eur'].sum().unstack(fill_value=0)
            for col in ['PALIWO', 'OPŁATA', 'INNE']:
                if col not in agg_baza.columns: agg_baza[col] = 0.0
            
            agg_baza['SUMA_BAZA'] = agg_baza['PALIWO'] + agg_baza['OPŁATA'] + agg_baza['INNE']
        else:
            agg_baza = pd.DataFrame(columns=['PALIWO', 'OPŁATA', 'INNE', 'SUMA_BAZA'])

        # 2. Dane z Pliku
        agg_analiza = pd.DataFrame()
        if plik_analizy:
            plik_analizy.seek(0)
            df_agreg, _ = przetworz_plik_analizy(plik_analizy, d_start, d_stop, wybrana_firma)
            if df_agreg is not None and not df_agreg.empty:
                # Filtr PTU i NONE dla pliku analizy
                df_agreg = df_agreg[~df_agreg.index.astype(str).str.replace(" ", "").str.contains("PTU0002", case=False)]
                df_agreg = df_agreg[df_agreg.index.astype(str).str.upper() != "NONE"]
                
                agg_analiza = df_agreg[['przychody_brutto', 'koszty_inne_brutto']].copy()
                agg_analiza.rename(columns={'koszty_inne_brutto': 'KOSZT_SUBIEKT', 'przychody_brutto': 'PRZYCHOD'}, inplace=True)
        
        if agg_analiza.empty:
            agg_analiza = pd.DataFrame(columns=['PRZYCHOD', 'KOSZT_SUBIEKT'])

        # 3. Merge
        df_full = agg_baza.join(agg_analiza, how='outer').fillna(0)
        df_full['ZYSK'] = df_full.get('PRZYCHOD', 0) - df_full.get('SUMA_BAZA', 0) - df_full.get('KOSZT_SUBIEKT', 0)
        df_full['KOSZTY_TOTAL'] = df_full.get('SUMA_BAZA', 0) + df_full.get('KOSZT_SUBIEKT', 0)
        
        return df_full

    # --- LOGIKA PRZYCISKU ---
    if 'por_data_ready' not in st.session_state:
        st.session_state.por_data_ready = False

    if st.button("🚀 Generuj Porównanie", type="primary", use_container_width=True):
        with st.spinner("Przetwarzanie danych..."):
            df_A = pobierz_agregacje(start_A, stop_A)
            df_B = pobierz_agregacje(start_B, stop_B)
            st.session_state.por_df_A = df_A
            st.session_state.por_df_B = df_B
            st.session_state.por_data_ready = True

    # --- WYŚWIETLANIE ---
    if st.session_state.por_data_ready:
        st.markdown("---")
        df_A = st.session_state.por_df_A
        df_B = st.session_state.por_df_B
        
        tab_wydatki, tab_przychody, tab_zyski = st.tabs(["💸 Wydatki", "💰 Przychody", "📈 Zyski"])

        # === TAB 1: WYDATKI ===
        with tab_wydatki:
            st.markdown("#### Porównanie Wydatków")
            opcje_kosztow = ['PALIWO', 'OPŁATA', 'INNE', 'KOSZT_SUBIEKT']
            wybrane_koszty = st.multiselect("Wybierz składniki kosztów:", opcje_kosztow, default=opcje_kosztow)
            
            if not wybrane_koszty:
                st.warning("Wybierz przynajmniej jedną kategorię.")
            else:
                df_A_view = df_A.copy()
                df_B_view = df_B.copy()
                df_A_view['FILTERED_COST'] = df_A_view[wybrane_koszty].sum(axis=1)
                df_B_view['FILTERED_COST'] = df_B_view[wybrane_koszty].sum(axis=1)
                
                sum_A = df_A_view['FILTERED_COST'].sum()
                sum_B = df_B_view['FILTERED_COST'].sum()
                diff = sum_A - sum_B
                delta_color = "inverse"

                c1, c2, c3 = st.columns(3)
                c1.metric("Wydatki Okres A", f"{sum_A:,.2f} EUR")
                c2.metric("Wydatki Okres B", f"{sum_B:,.2f} EUR")
                c3.metric("Różnica (A - B)", f"{diff:,.2f} EUR", delta=f"{diff:,.2f} EUR", delta_color=delta_color)

                df_merge = df_A_view[['FILTERED_COST']].join(df_B_view[['FILTERED_COST']], lsuffix='_A', rsuffix='_B', how='outer').fillna(0)
                df_merge['Różnica'] = df_merge['FILTERED_COST_A'] - df_merge['FILTERED_COST_B']
                df_merge = df_merge.sort_values(by='FILTERED_COST_A', ascending=False)
                
                # --- MODYFIKACJA LP ---
                df_merge_show = df_merge.reset_index()
                df_merge_show.insert(0, 'Lp.', range(1, 1 + len(df_merge_show)))

                st.dataframe(
                    df_merge_show.style.format("{:,.2f} EUR", subset=['FILTERED_COST_A', 'FILTERED_COST_B', 'Różnica']).applymap(color_negative_good, subset=['Różnica']),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "identyfikator_clean": "Pojazd",
                        "FILTERED_COST_A": st.column_config.NumberColumn("Koszt A", format="%.2f EUR"),
                        "FILTERED_COST_B": st.column_config.NumberColumn("Koszt B", format="%.2f EUR"),
                        "Różnica": st.column_config.NumberColumn("Zmiana", format="%.2f EUR")
                    }
                )

        # === TAB 2: PRZYCHODY ===
        with tab_przychody:
            st.markdown("#### Porównanie Przychodów")
            rev_A = df_A['PRZYCHOD'].sum()
            rev_B = df_B['PRZYCHOD'].sum()
            diff_rev = rev_A - rev_B
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Przychód Okres A", f"{rev_A:,.2f} EUR")
            c2.metric("Przychód Okres B", f"{rev_B:,.2f} EUR")
            c3.metric("Różnica (A - B)", f"{diff_rev:,.2f} EUR", delta=f"{diff_rev:,.2f} EUR")

            df_merge_rev = df_A[['PRZYCHOD']].join(df_B[['PRZYCHOD']], lsuffix='_A', rsuffix='_B', how='outer').fillna(0)
            df_merge_rev['Różnica'] = df_merge_rev['PRZYCHOD_A'] - df_merge_rev['PRZYCHOD_B']
            df_merge_rev = df_merge_rev.sort_values(by='PRZYCHOD_A', ascending=False)
            
            # --- MODYFIKACJA LP ---
            df_merge_rev_show = df_merge_rev.reset_index()
            df_merge_rev_show.insert(0, 'Lp.', range(1, 1 + len(df_merge_rev_show)))

            st.dataframe(
                df_merge_rev_show.style.format("{:,.2f} EUR").applymap(color_positive_good, subset=['Różnica']),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "identyfikator_clean": "Pojazd",
                    "PRZYCHOD_A": st.column_config.NumberColumn("Przychód A", format="%.2f EUR"),
                    "PRZYCHOD_B": st.column_config.NumberColumn("Przychód B", format="%.2f EUR"),
                    "Różnica": st.column_config.NumberColumn("Zmiana", format="%.2f EUR")
                }
            )

        # === TAB 3: ZYSKI ===
        with tab_zyski:
            st.markdown("#### Porównanie Zysków (Brutto)")
            zysk_A = df_A['ZYSK'].sum()
            zysk_B = df_B['ZYSK'].sum()
            diff_zysk = zysk_A - zysk_B
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Zysk Okres A", f"{zysk_A:,.2f} EUR")
            c2.metric("Zysk Okres B", f"{zysk_B:,.2f} EUR")
            c3.metric("Różnica (A - B)", f"{diff_zysk:,.2f} EUR", delta=f"{diff_zysk:,.2f} EUR")

            df_merge_zysk = df_A[['ZYSK']].join(df_B[['ZYSK']], lsuffix='_A', rsuffix='_B', how='outer').fillna(0)
            df_merge_zysk['Różnica'] = df_merge_zysk['ZYSK_A'] - df_merge_zysk['ZYSK_B']
            df_merge_zysk = df_merge_zysk.sort_values(by='ZYSK_A', ascending=False)
            
            # --- MODYFIKACJA LP ---
            df_merge_zysk_show = df_merge_zysk.reset_index()
            df_merge_zysk_show.insert(0, 'Lp.', range(1, 1 + len(df_merge_zysk_show)))
            
            st.dataframe(
                df_merge_zysk_show.style.format("{:,.2f} EUR").applymap(color_positive_good, subset=['ZYSK_A', 'ZYSK_B', 'Różnica']),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "identyfikator_clean": "Pojazd",
                    "ZYSK_A": st.column_config.NumberColumn("Zysk A", format="%.2f EUR"),
                    "ZYSK_B": st.column_config.NumberColumn("Zysk B", format="%.2f EUR"),
                    "Różnica": st.column_config.NumberColumn("Zmiana", format="%.2f EUR")
                }
            )
# --- GŁÓWNA APLIKACJA ---
def main_app():
    if 'active_company' not in st.session_state: st.session_state.active_company = FIRMY[0]
    if 'active_view' not in st.session_state: st.session_state.active_view = 'Raport'
    if 'show_admin' not in st.session_state: st.session_state.show_admin = False

    with st.sidebar:
        st.markdown("### 🏢 Kontekst")
        c1, c2 = st.columns(2)
        
        type_holier = "primary" if st.session_state.active_company == "HOLIER" else "secondary"
        if c1.button("HOLIER", type=type_holier, use_container_width=True):
            st.session_state.active_company = "HOLIER"
            st.rerun()
            
        type_unix = "primary" if st.session_state.active_company == "UNIX-TRANS" else "secondary"
        if c2.button("UNIX-TRANS", type=type_unix, use_container_width=True):
            st.session_state.active_company = "UNIX-TRANS"
            st.rerun()
            
        st.caption(f"Baza: {NAZWA_POLACZENIA_DB}")
        st.divider()
        
        st.markdown("### 📊 Nawigacja")
        
        is_admin = st.session_state.show_admin
        
        raport_type = "primary" if st.session_state.active_view == 'Raport' and not is_admin else "secondary"
        if st.button("Raport Paliw/Opłat", type=raport_type, use_container_width=True):
            st.session_state.active_view = 'Raport'
            st.session_state.show_admin = False
            st.rerun()
            
        rent_type = "primary" if st.session_state.active_view == 'Rentowność' and not is_admin else "secondary"
        if st.button("Rentowność", type=rent_type, use_container_width=True):
            st.session_state.active_view = 'Rentowność'
            st.session_state.show_admin = False
            st.rerun()
            
        ref_type = "primary" if st.session_state.active_view == 'Refaktury' and not is_admin else "secondary"
        if st.button("Refaktury", type=ref_type, use_container_width=True):
            st.session_state.active_view = 'Refaktury'
            st.session_state.show_admin = False
            st.rerun()
        
        comp_type = "primary" if st.session_state.active_view == 'Porównanie' and not is_admin else "secondary"
        if st.button("Porównanie", type=comp_type, use_container_width=True):
            st.session_state.active_view = 'Porównanie'
            st.session_state.show_admin = False
            st.rerun()

        st.markdown("<div style='height: 50px;'></div>", unsafe_allow_html=True)
        st.divider()
        
        admin_type = "primary" if is_admin else "secondary"
        if st.button("⚙️ Panel Administratora", type=admin_type, use_container_width=True):
            st.session_state.show_admin = True
            st.rerun()

    try: conn = st.connection(NAZWA_POLACZENIA_DB, type="sql")
    except Exception as e: st.error("Błąd połączenia z DB."); st.stop()

    firma = st.session_state.active_company
    
    st.title(f"Analizator Wydatków: {firma}")
    
    if st.session_state.show_admin:
        render_admin_content(conn, firma)
    else:
        if st.session_state.active_view == 'Raport':
            render_raport_content(conn, firma)
        elif st.session_state.active_view == 'Rentowność':
            render_rentownosc_content(conn, firma)
        elif st.session_state.active_view == 'Refaktury':
            render_refaktury_content(conn, firma)
        elif st.session_state.active_view == 'Porównanie':  
            render_porownanie_content(conn, firma)

def check_password():
    try:
        prawidlowe_haslo = st.secrets["ADMIN_PASSWORD"]
    except:
        st.error("Błąd krytyczny: Nie ustawiono 'ADMIN_PASSWORD'.")
        st.stop()
    if st.session_state.get("password_correct", False):
        return True
    
    col1, col2, col3 = st.columns([1,1,1])
    with col2:
        with st.form("login"):
            st.title("🔒 Logowanie")
            wpisane_haslo = st.text_input("Podaj hasło administratora", type="password")
            submitted = st.form_submit_button("Zaloguj", use_container_width=True)
            if submitted:
                if wpisane_haslo == prawidlowe_haslo:
                    st.session_state["password_correct"] = True
                    st.rerun() 
                else:
                    st.error("Nieprawidłowe hasło.")
    return False

if check_password():
    main_app()
