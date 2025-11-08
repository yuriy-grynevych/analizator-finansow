import pandas as pd
import numpy as np
import requests
import re
import streamlit as st
import time
from datetime import date
from sqlalchemy import text 

# --- USTAWIENIA STRONY ---
st.set_page_config(page_title="Analizator Wydatków", layout="wide")

# --- KOD DO UKRYCIA STOPKI I MENU ---
hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

# --- PARAMETRY TABELI ---
NAZWA_TABELI = "transactions"
NAZWA_SCHEMATU = "public"
NAZWA_POLACZENIA_DB = "db" 

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

# --- FUNKCJE "TŁUMACZENIA" (BEZ ZMIAN) ---
def normalizuj_eurowag(df_eurowag):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_eurowag['Data i godzina'], errors='coerce')
    df_out['identyfikator'] = df_eurowag['Tablica rejestracyjna'].fillna(df_eurowag['Karta'])
    df_out['kwota_brutto'] = pd.to_numeric(df_eurowag['Kwota brutto'], errors='coerce')
    df_out['waluta'] = df_eurowag['Waluta']
    df_out['zrodlo'] = 'Eurowag'
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

def normalizuj_e100(df_e100):
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_e100['Data'] + ' ' + df_e100['Czas'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    df_out['identyfikator'] = df_e100['Numer samochodu'].fillna(df_e100['Numer karty'])
    df_out['kwota_brutto'] = pd.to_numeric(df_e100['Kwota'], errors='coerce')
    df_out['waluta'] = df_e100['Waluta']
    df_out['zrodlo'] = 'E100'
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

# --- FUNKCJA DO WCZYTYWANIA PLIKÓW (BEZ ZMIAN) ---
def wczytaj_i_zunifikuj_pliki(przeslane_pliki):
    lista_df_zunifikowanych = []
    for plik in przeslane_pliki:
        st.write(f" - Przetwarzam: {plik.name}")
        try:
            if plik.name.endswith('.csv'):
                pass 
            elif plik.name.endswith(('.xls', '.xlsx')):
                df_pierwszy_arkusz = pd.read_excel(plik, engine='openpyxl')
                kolumny_pierwszego = df_pierwszy_arkusz.columns
                if 'Data i godzina' in kolumny_pierwszego and 'Artykuł' in kolumny_pierwszego:
                    lista_df_zunifikowanych.append(normalizuj_eurowag(df_pierwszy_arkusz))
                else:
                    try:
                        df_arkusz_e100 = pd.read_excel(plik, sheet_name='Transactions', engine='openpyxl')
                        kolumny_e100 = df_arkusz_e100.columns
                        if 'Numer samochodu' in kolumny_e100 and 'Numer karty' in kolumny_e100:
                            lista_df_zunifikowanych.append(normalizuj_e100(df_arkusz_e100))
                    except Exception as e:
                        st.warning(f"Pominięto plik {plik.name}. Nie rozpoznano formatu.")
        except Exception as e:
             st.error(f"BŁĄD wczytania pliku {plik.name}: {e}")
    
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
                kwota_brutto FLOAT,
                waluta VARCHAR(10),
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
        );
        """))
        s.commit()

def pobierz_dane_z_bazy(conn, data_start, data_stop):
    start_datetime = pd.to_datetime(data_start)
    stop_datetime = pd.to_datetime(data_stop) + pd.Timedelta(days=1)
    
    query = f"""
        SELECT data_transakcji, identyfikator, kwota_brutto, waluta 
        FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}
        WHERE data_transakcji >= :data_start AND data_transakcji < :data_stop
    """
    
    df = conn.query(query, params={"data_start": start_datetime, "data_stop": stop_datetime})
    return df

# --- FUNKCJA main() (Z USUNIĘTYM SUBHEADEREM) ---
def main_app():
    
    st.title("Analizator Wydatków Floty (Eurowag + E100)")
    # Usunięta linijka st.subheader(...)
    
    tab_raport, tab_admin = st.tabs(["📊 Raport Główny", "⚙️ Panel Admina"])

    try:
        conn = st.connection(NAZWA_POLACZENIA_DB, type="sql")
    except Exception as e:
        st.error(f"Nie udało się połączyć z bazą danych '{NAZWA_POLACZENIA_DB}'. Sprawdź 'Secrets' w Ustawieniach.")
        st.stop() 

    with tab_raport:
        st.header("Raport Wydatków")
        
        try:
            min_max_date = conn.query(f"SELECT MIN(data_transakcji), MAX(data_transakcji) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}")
            
            if min_max_date.empty or min_max_date.iloc[0, 0] is None:
                st.info("Baza danych jest pusta. Przejdź do Panelu Admina, aby wgrać pliki.")
            else:
                domyslny_start = min_max_date.iloc[0, 0].date()
                domyslny_stop = min_max_date.iloc[0, 1].date()

                col1, col2 = st.columns(2)
                with col1:
                    data_start = st.date_input("Data Start", value=domyslny_start, min_value=domyslny_start, max_value=domyslny_stop)
                with col2:
                    data_stop = st.date_input("Data Stop", value=domyslny_stop, min_value=domyslny_start, max_value=domyslny_stop)

                dane_z_bazy = pobierz_dane_z_bazy(conn, data_start, data_stop)
                
                if dane_z_bazy.empty:
                    st.warning(f"Brak danych w wybranym zakresie dat ({data_start} - {data_stop}).")
                else:
                    kurs_eur = pobierz_kurs_eur_pln()
                    if kurs_eur:
                        unikalne_waluty = dane_z_bazy['waluta'].unique()
                        mapa_kursow = pobierz_wszystkie_kursy(unikalne_waluty, kurs_eur)
                        
                        dane_z_bazy['identyfikator_clean'] = dane_z_bazy['identyfikator'].astype(str).str.extract(r'([A-Z0-9]{4,})')
                        dane_z_bazy['identyfikator_clean'] = dane_z_bazy['identyfikator_clean'].fillna('Brak Identyfikatora')
                        dane_z_bazy['kwota_brutto_num'] = pd.to_numeric(dane_z_bazy['kwota_brutto'], errors='coerce').fillna(0.0)
                        dane_z_bazy['kurs_do_eur'] = dane_z_bazy['waluta'].map(mapa_kursow).fillna(0.0)
                        dane_z_bazy['kwota_finalna_eur'] = dane_z_bazy['kwota_brutto_num'] * dane_z_bazy['kurs_do_eur']
                        
                        podsumowanie = dane_z_bazy.groupby('identyfikator_clean')['kwota_finalna_eur'].sum().sort_values(ascending=False)
                        df_wynik = pd.DataFrame(podsumowanie)
                        df_wynik.rename(columns={'kwota_finalna_eur': 'Łączne wydatki (EUR)'}, inplace=True)
                        df_wynik.index.name = 'Identyfikator (Pojazd / Karta)'
                        
                        suma_laczna = df_wynik['Łączne wydatki (EUR)'].sum()
                        st.metric(label="SUMA ŁĄCZNA (dla wybranego okresu)", value=f"{suma_laczna:,.2f} EUR")
                        st.dataframe(df_wynik.style.format("{:,.2f} EUR", subset=['Łączne wydatki (EUR)']), use_container_width=True)

        except Exception as e:
            if "does not exist" in str(e):
                 st.warning("Baza danych jest pusta lub nie została jeszcze utworzona. Przejdź do 'Panelu Admina', aby ją zainicjować.")
            else:
                 st.error(f"Wystąpił nieoczekiwany błąd w zakładce raportu: {e}")

    with tab_admin:
        st.header("Panel Administracyjny")
        
        st.success("Zalogowano pomyślnie!")

        if st.button("1. Stwórz tabelę w bazie danych (tylko raz!)"):
            with st.spinner("Tworzenie tabeli..."):
                setup_database(conn)
            st.success("Tabela 'transactions' jest gotowa.")

        st.subheader("Wgrywanie nowych plików")
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

# --- LOGIKA LOGOWANIA (BEZ ZMIAN) ---
def check_password():
    try:
        prawidlowe_haslo = st.secrets["ADMIN_PASSWORD"]
    except:
        st.error("Błąd krytyczny: Nie ustawiono 'ADMIN_PASSWORD' w Ustawieniach (Secrets) aplikacji.")
        st.stop()

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
