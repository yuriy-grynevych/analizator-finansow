import pandas as pd
import numpy as np
import requests
import re
import streamlit as st
import time
from datetime import date
from sqlalchemy import text 
import io # Potrzebne do eksportu Excela

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
NAZWA_SCHEMATU = "public"
NAZWA_POLACZENIA_DB = "db" 

# --- SŁOWNIK VAT ---
VAT_RATES = {
    "PL": 0.23, "DE": 0.19, "CZ": 0.21, "AT": 0.20, "FR": 0.20,
    "DK": 0.25, "NL": 0.21, "BE": 0.21, "ES": 0.21, "IT": 0.22,
}

# --- LISTY DO PARSOWANIA PLIKU 'analiza.xlsx' ---
ETYKIETY_PRZYCHODOW = [
    'Faktura VAT sprzedaży', 'Korekta faktury VAT zakupu', 'Przychód wewnętrzny'
]
ETYKIETY_KOSZTOW_INNYCH = [
    'Faktura VAT zakupu', 'Korekta faktury VAT sprzedaży', 'Art. biurowe', 
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
        artykul = str(row.get('Artykuł', '')).strip() # Zostawiamy oryginalny opis
        
        if 'TOLL' in usluga.upper() or 'OPŁATA DROGOWA' in usluga.upper():
            return 'OPŁATA', artykul # Zwraca prawdziwy opis
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
    
# --- NOWE FUNKCJE "TŁUMACZENIA" (ZMIANA DLA EUROWAG) ---
def normalizuj_eurowag(df_eurowag):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_eurowag['Data i godzina'], errors='coerce')
    
    # --- NOWA, LEPSZA LOGIKA IDENTYFIKATORA ---
    # 1. Spróbuj 'Tablica rejestracyjna'
    # 2. Jeśli puste, spróbuj 'Posiadacz karty' (bo tam jest np. "PL WGM0502K")
    # 3. Jeśli puste, w ostateczności weź 'Karta'
    df_out['identyfikator'] = df_eurowag['Tablica rejestracyjna'].fillna(
                                df_eurowag['Posiadacz karty'].fillna(df_eurowag['Karta'])
                            )
    # --- KONIEC ZMIANY ---
    
    df_out['kwota_netto'] = pd.to_numeric(df_eurowag['Kwota netto'], errors='coerce')
    df_out['kwota_brutto'] = pd.to_numeric(df_eurowag['Kwota brutto'], errors='coerce')
    df_out['waluta'] = df_eurowag['Waluta']
    df_out['ilosc'] = pd.to_numeric(df_eurowag['Ilość'], errors='coerce')
    df_out['zrodlo'] = 'Eurowag'
    
    kategorie = df_eurowag.apply(lambda row: kategoryzuj_transakcje(row, 'Eurowag'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie]
    df_out['produkt'] = [kat[1] for kat in kategorie]

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
    df_out['zrodlo'] = 'E100_PL'
    
    kategorie = df_e100.apply(lambda row: kategoryzuj_transakcje(row, 'E100_PL'), axis=1)
    df_out['typ'] = [kat[0] for kat in kategorie]
    df_out['produkt'] = [kat[1] for kat in kategorie]
    
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

# --- FUNKCJA DO WCZYTYWANIA PLIKÓW (ZMIANA DETEKTORA EUROWAG) ---
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
                
                # --- ZMIANA: Sprawdzamy 'Posiadacz karty' ---
                elif 'Sheet0' in xls.sheet_names or len(xls.sheet_names) > 0:
                    df_eurowag = pd.read_excel(xls, sheet_name=0) 
                    kolumny_eurowag = df_eurowag.columns
                    if 'Data i godzina' in kolumny_eurowag and 'Posiadacz karty' in kolumny_eurowag:
                        st.write("   -> Wykryto format Eurowag (Nowy)")
                        lista_df_zunifikowanych.append(normalizuj_eurowag(df_eurowag))
                    # Awaryjnie dla starszych plików
                    elif 'Data i godzina' in kolumny_eurowag and 'Artykuł' in kolumny_eurowag:
                         st.write("   -> Wykryto format Eurowag (Starszy)")
                         if 'Posiadacz karty' not in df_eurowag.columns:
                             df_eurowag['Posiadacz karty'] = None # Wypełnij brakującą kolumnę
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

# --- FUNKCJE BAZY DANYCH (BEZ ZMIAN) ---
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

# --- NOWA FUNKCJA PRZYGOTOWUJĄCA DANE PALIWOWE ---
def przygotuj_dane_paliwowe(dane_z_bazy):
    if dane_z_bazy.empty:
        return dane_z_bazy, None
        
    dane_z_bazy['data_transakcji_dt'] = pd.to_datetime(dane_z_bazy['data_transakcji'])
    
    identyfikatory = dane_z_bazy['identyfikator'].astype(str)
    
    def clean_key(key):
        if key == 'nan' or not key: 
            return 'Brak Identyfikatora'
        match = re.search(r'([A-Z0-9]{4,})', key)
        if match:
            return match.group(1).upper().strip()
        return 'Brak Identyfikatora'
        
    dane_z_bazy['identyfikator_clean'] = identyfikatory.apply(clean_key)
    
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
    
    return dane_z_bazy, mapa_kursow

# --- FUNKCJA PARSOWANIA 'analiza.xlsx' (POPRAWIONY BŁĄD 'PUSHED') ---
@st.cache_data 
def przetworz_plik_analizy(przeslany_plik):
    st.write("Przetwarzanie pliku `analiza.xlsx`...")
    try:
        df = pd.read_excel(przeslany_plik, 
                           sheet_name='pojazdy', 
                           engine='openpyxl', 
                           header=7) 
    except Exception as e:
        st.error(f"Nie udało się wczytać arkusza 'pojazdy' z pliku `analiza.xlsx`. Błąd: {e}")
        return None

    df = df.dropna(subset=['Etykiety wierszy'])
    
    wyniki = []
    aktualny_pojazd_oryg = None
    
    for index, row in df.iterrows():
        etykieta = str(row['Etykiety wierszy']).strip()
        kwota_euro = row.get('euro', row.get('EUR', 0.0)) 

        if etykieta not in WSZYSTKIE_ZNANE_ETYKIETY:
            aktualny_pojazd_oryg = etykieta
            continue 

        if aktualny_pojazd_oryg is not None and pd.notna(kwota_euro):
            if etykieta in ETYKIETY_PRZYCHODOW:
                wyniki.append({
                    'pojazd_oryg': aktualny_pojazd_oryg,
                    'przychody': kwota_euro,
                    'koszty_inne': 0
                })
            elif etykieta in ETYKIETY_KOSZTOW_INNYCH:
                 wyniki.append({
                    'pojazd_oryg': aktualny_pojazd_oryg,
                    'przychody': 0,
                    'koszty_inne': kwota_euro 
                })
            
    if not wyniki:
        st.error("Nie znaleziono żadnych danych o przychodach/kosztach w pliku `analiza.xlsx`.")
        return None

    df_wyniki = pd.DataFrame(wyniki)
    
    df_wyniki['pojazd_clean'] = df_wyniki['pojazd_oryg'].astype(str).apply(
        lambda x: re.search(r'([A-Z0-9]{4,})', str(x).upper()).group(1).strip() 
        if re.search(r'([A-Z0-9]{4,})', str(x).upper()) else 'Brak Identyfikatora'
    )

    df_agregacja = df_wyniki.groupby('pojazd_clean')[['przychody', 'koszty_inne']].sum()
    
    st.success("Plik `analiza.xlsx` przetworzony pomyślnie.")
    return df_agregacja


# --- FUNKCJA main() (ZE ZMIANAMI) ---
def main_app():
    
    st.title("Analizator Wydatków Floty") 
    
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

        if st.button("1. Stwórz tabelę w bazie danych (tylko raz!)"):
            with st.spinner("Tworzenie tabeli..."):
                setup_database(conn)
            st.success("Tabela 'transactions' jest gotowa.")

        st.subheader("Wgrywanie nowych plików (Paliwo/Opłaty)")
        przeslane_pliki = st.file_uploader(
            "Wybierz pliki Eurowag i E100 do dodania do bazy",
            accept_multiple_files=True,
            type=['xlsx', 'xls']
        )
        
        if przeslane_pliki:
            if st.button("2. Przetwórz i wgraj pliki do bazy", type="primary"):
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

                            st.metric(
                                label="Suma Łączna (Paliwo)", 
                                value=f"{podsumowanie_paliwo['Kwota_Brutto_EUR'].sum():,.2f} EUR"
                            )
                            
                            kolumny_do_pokazania = ['Kwota_Netto_EUR', 'Kwota_Brutto_EUR', 'Litry (Diesel)', 'Litry (AdBlue)']
                            formatowanie = {
                                'Kwota_Netto_EUR': '{:,.2f} EUR',
                                'Kwota_Brutto_EUR': '{:,.2f} EUR',
                                'Litry (Diesel)': '{:,.2f} L',
                                'Litry (AdBlue)': '{:,.2f} L'
                            }

                            st.dataframe(
                                podsumowanie_paliwo[kolumny_do_pokazania].style.format(formatowanie), 
                                use_container_width=True
                            )
                            
                            st.download_button(
                                label="Pobierz raport jako Excel (.xlsx)",
                                data=to_excel(podsumowanie_paliwo),
                                file_name=f"raport_paliwo_{data_start_rap}_do_{data_stop_rap}.xlsx",
                                mime="application/vnd.ms-excel"
                            )
                            
                            st.divider()
                            st.subheader("Szczegóły transakcji paliwowych")
                            lista_pojazdow_paliwo = ["--- Wybierz pojazd ---"] + list(podsumowanie_paliwo.index)
                            wybrany_pojazd_paliwo = st.selectbox("Wybierz identyfikator:", lista_pojazdow_paliwo)
                            
                            if wybrany_pojazd_paliwo != "--- Wybierz pojazd ---":
                                df_szczegoly = df_paliwo[df_paliwo['identyfikator_clean'] == wybrany_pojazd_paliwo].sort_values(by='data_transakcji_dt', ascending=False)
                                df_szczegoly_display = df_szczegoly[['data_transakcji_dt', 'produkt', 'ilosc', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                st.dataframe(
                                    df_szczegoly_display.rename(columns={
                                        'data_transakcji_dt': 'Data', 'produkt': 'Produkt', 'ilosc': 'Litry',
                                        'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'
                                    }),
                                    use_container_width=True, hide_index=True,
                                    column_config={
                                        "Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
                                        "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),
                                        "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),
                                        "Litry": st.column_config.NumberColumn(format="%.2f L"),
                                    }
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

                            st.metric(
                                label="Suma Łączna (Opłaty Drogowe)", 
                                value=f"{podsumowanie_oplaty['Kwota_Brutto_EUR'].sum():,.2f} EUR"
                            )
                            st.dataframe(
                                podsumowanie_oplaty.style.format({
                                    'Kwota_Netto_EUR': '{:,.2f} EUR',
                                    'Kwota_Brutto_EUR': '{:,.2f} EUR'
                                }), use_container_width=True
                            )
                            st.download_button(
                                label="Pobierz raport jako Excel (.xlsx)",
                                data=to_excel(podsumowanie_oplaty),
                                file_name=f"raport_oplaty_{data_start_rap}_do_{data_stop_rap}.xlsx",
                                mime="application/vnd.ms-excel"
                            )
                            
                            st.divider()
                            st.subheader("Szczegóły transakcji (Opłaty)")
                            lista_pojazdow_oplaty = ["--- Wybierz pojazd ---"] + list(podsumowanie_oplaty.index)
                            wybrany_pojazd_oplaty = st.selectbox("Wybierz identyfikator:", lista_pojazdow_oplaty, key="select_oplaty")
                            
                            if wybrany_pojazd_oplaty != "--- Wybierz pojazd ---":
                                df_szczegoly_oplaty = df_oplaty[df_oplaty['identyfikator_clean'] == wybrany_pojazd_oplaty].sort_values(by='data_transakcji_dt', ascending=False)
                                df_szczegoly_oplaty_display = df_szczegoly_oplaty[['data_transakcji_dt', 'produkt', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                st.dataframe(
                                    df_szczegoly_oplaty_display.rename(columns={
                                        'data_transakcji_dt': 'Data', 'produkt': 'Opis',
                                        'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'
                                    }),
                                    use_container_width=True, hide_index=True,
                                    column_config={
                                        "Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
                                        "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),
                                        "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),
                                    }
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
                            
                            st.metric(
                                label="Suma Łączna (Pozostałe)", 
                                value=f"{podsumowanie_inne['Kwota_Brutto_EUR'].sum():,.2f} EUR"
                            )
                            st.dataframe(
                                podsumowanie_inne.style.format({
                                    'Kwota_Netto_EUR': '{:,.2f} EUR',
                                    'Kwota_Brutto_EUR': '{:,.2f} EUR'
                                }), use_container_width=True
                            )
                            st.download_button(
                                label="Pobierz raport jako Excel (.xlsx)",
                                data=to_excel(podsumowanie_inne),
                                file_name=f"raport_inne_{data_start_rap}_do_{data_stop_rap}.xlsx",
                                mime="application/vnd.ms-excel"
                            )

                            st.divider()
                            st.subheader("Szczegóły transakcji (Inne)")
                            lista_pojazdow_inne = ["--- Wybierz pojazd ---"] + list(podsumowanie_inne.index)
                            wybrany_pojazd_inne = st.selectbox("Wybierz identyfikator:", lista_pojazdow_inne, key="select_inne")
                            
                            if wybrany_pojazd_inne != "--- Wybierz pojazd ---":
                                df_szczegoly_inne = df_inne[df_inne['identyfikator_clean'] == wybrany_pojazd_inne].sort_values(by='data_transakcji_dt', ascending=False)
                                df_szczegoly_inne_display = df_szczegoly_inne[['data_transakcji_dt', 'produkt', 'kwota_brutto_eur', 'kwota_netto_eur', 'zrodlo']]
                                st.dataframe(
                                    df_szczegoly_inne_display.rename(columns={
                                        'data_transakcji_dt': 'Data', 'produkt': 'Opis',
                                        'kwota_brutto_eur': 'Brutto (EUR)', 'kwota_netto_eur': 'Netto (EUR)', 'zrodlo': 'System'
                                    }),
                                    use_container_width=True, hide_index=True,
                                    column_config={
                                        "Data": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
                                        "Brutto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),
                                        "Netto (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),
                                    }
                                )

        except Exception as e:
            if "does not exist" in str(e):
                 st.warning("Baza danych jest pusta lub nie została jeszcze utworzona. Przejdź do 'Panelu Admina', aby ją zainicjować.")
            else:
                 st.error(f"Wystąpił nieoczekiwany błąd w zakładce raportu: {e}")
                 st.exception(e) 

    # --- ZAKŁADKA 3: RENTOWNOŚĆ (TERAZ TRZECIA) ---
    with tab_rentownosc:
        st.header("Raport Rentowności (Zysk/Strata)")
        try:
            min_max_date_query = f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}"
            min_max_date = conn.query(min_max_date_query)
            if min_max_date.empty or min_max_date.iloc[0, 0] is None:
                st.info("Baza danych paliwowych jest pusta. Przejdź do Panelu Admina, aby wgrać pliki.")
            else:
                domyslny_start_rent = min_max_date.iloc[0, 0]
                domyslny_stop_rent = min_max_date.iloc[0, 1]
                
                col1_rent, col2_rent = st.columns(2)
                with col1_rent:
                    data_start_rent = st.date_input("Data Start", value=domyslny_start_rent, min_value=domyslny_start_rent, max_value=domyslny_stop_rent, key="rent_start")
                with col2_rent:
                    data_stop_rent = st.date_input("Data Stop", value=domyslny_stop_rent, min_value=domyslny_start_rent, max_value=domyslny_stop_rent, key="rent_stop")

                plik_analizy = st.file_uploader("Prześlij plik `analiza.xlsx` (ten z Subiekta)", type=['xlsx'])
                
                if 'raport_gotowy' not in st.session_state:
                    st.session_state['raport_gotowy'] = False

                if st.button("Generuj raport rentowności", type="primary"):
                    if plik_analizy is None:
                        st.warning("Proszę, prześlij plik `analiza.xlsx`.")
                        st.session_state['raport_gotowy'] = False 
                    else:
                        with st.spinner("Pracuję..."):
                            dane_z_bazy_rent = pobierz_dane_z_bazy(conn, data_start_rent, data_stop_rent, typ='PALIWO') 
                            
                            if dane_z_bazy_rent.empty:
                                st.error("Brak danych paliwowych w wybranym okresie.")
                                st.session_state['raport_gotowy'] = False
                            else:
                                dane_przygotowane_rent, _ = przygotuj_dane_paliwowe(dane_z_bazy_rent.copy())
                                
                                if dane_przygotowane_rent is None: 
                                    st.session_state['raport_gotowy'] = False
                                else:
                                    df_koszty_paliwa = dane_przygotowane_rent.groupby('identyfikator_clean')['kwota_finalna_eur'].sum().to_frame('Koszty Paliwa (z Bazy)')
                                    df_analiza = przetworz_plik_analizy(plik_analizy)
                                    
                                    if df_analiza is not None:
                                        df_rentownosc = df_analiza.merge(
                                            df_koszty_paliwa, 
                                            left_index=True, 
                                            right_index=True, 
                                            how='outer'
                                        ).fillna(0)
                                        
                                        df_rentownosc['ZYSK / STRATA (EUR)'] = (
                                            df_rentownosc['przychody'] - 
                                            df_rentownosc['koszty_inne'] - 
                                            df_rentownosc['Koszty Paliwa (z Bazy)']
                                        )
                                        
                                        st.session_state['raport_gotowy'] = True
                                        st.session_state['df_rentownosc'] = df_rentownosc
                                        st.session_state['wybrany_pojazd_rent'] = "--- Wybierz pojazd ---" 
                        
                if st.session_state.get('raport_gotowy', False):
                    st.subheader("Wyniki dla wybranego okresu")
                    df_rentownosc = st.session_state['df_rentownosc']
                    df_rentownosc = df_rentownosc.sort_values(by='ZYSK / STRATA (EUR)', ascending=False)
                    
                    lista_pojazdow_rent = ["--- Wybierz pojazd ---"] + list(df_rentownosc.index.unique())
                    
                    wybrany_pojazd_rent = st.selectbox(
                        "Wybierz pojazd do analizy:", 
                        lista_pojazdow_rent,
                        key='wybrany_pojazd_rent'
                    )
                    
                    if wybrany_pojazd_rent != "--- Wybierz pojazd ---":
                        try:
                            dane_pojazdu = df_rentownosc.loc[wybrany_pojazd_rent]
                            przychody = dane_pojazdu['przychody']
                            koszty_inne = dane_pojazdu['koszty_inne']
                            koszty_paliwa = dane_pojazdu['Koszty Paliwa (z Bazy)']
                            zysk = dane_pojazdu['ZYSK / STRATA (EUR)']
                            
                            delta_color = "normal"
                            if zysk < 0: delta_color = "inverse"
                            
                            st.metric(label="ZYSK / STRATA (EUR)", value=f"{zysk:,.2f} EUR", delta_color=delta_color)
                            
                            col1, col2, col3 = st.columns(3)
                            col1.metric("Przychód (z Subiekta)", f"{przychody:,.2f} EUR")
                            col2.metric("Koszty Inne (z Subiekta)", f"{-koszty_inne:,.2f} EUR")
                            col3.metric("Koszty Paliwa (z Bazy)", f"{-koszty_paliwa:,.2f} EUR")
                        
                        except KeyError:
                            st.error("Nie znaleziono danych dla tego pojazdu.")
                    
                    st.divider()
                    zysk_laczny = df_rentownosc['ZYSK / STRATA (EUR)'].sum()
                    st.metric(label="SUMA ŁĄCZNA (ZYSK/STRATA)", value=f"{zysk_laczny:,.2f} EUR")
                    
                    df_rentownosc_display = df_rentownosc[[
                        'przychody', 
                        'koszty_inne', 
                        'Koszty Paliwa (z Bazy)',
                        'ZYSK / STRATA (EUR)'
                    ]].rename(columns={
                        'przychody': 'Przychód (Subiekt)',
                        'koszty_inne': 'Koszty Inne (Subiekt)'
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
                 st.warning("Baza danych jest pusta lub nie została jeszcze utworzona. Przejdź do 'Panelu Admina', aby ją zainicjować.")
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
