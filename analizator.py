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
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

# --- PARAMETRY TABELI ---
NAZWA_TABELI = "transactions"
NAZWA_SCHEMATU = "public"
NAZWA_POLACZENIA_DB = "db" 

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
    query = f"""
        SELECT data_transakcji, identyfikator, kwota_brutto, waluta, zrodlo 
        FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}
        WHERE (data_transakcji::date) >= :data_start 
          AND (data_transakcji::date) <= :data_stop
    """
    df = conn.query(query, params={"data_start": data_start, "data_stop": data_stop})
    return df

# --- FUNKCJA PARSOWANIA 'analiza.xlsx' (BEZ ZMIAN) ---
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
        kwota_euro = row['euro'] 

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
    df_wyniki['pojazd_clean'] = df_wyniki['pojazd_oryg'].astype(str).str.extract(r'([A-Z0-9]{4,})').str.upper().str.strip()
    df_agregacja = df_wyniki.groupby('pojazd_clean')[['przychody', 'koszty_inne']].sum()
    
    st.success("Plik `analiza.xlsx` przetworzony pomyślnie.")
    return df_agregacja


# --- FUNKCJA main() (Z POPRAWKĄ) ---
def main_app():
    
    st.title("Analizator Wydatków Floty") 
    
    tab_raport, tab_rentownosc, tab_admin = st.tabs([
        "📊 Raport Paliwowy", 
        "💰 Rentowność (Zysk/Strata)", 
        "⚙️ Panel Admina"
    ])

    try:
        conn = st.connection(NAZWA_POLACZENIA_DB, type="sql")
    except Exception as e:
        st.error(f"Nie udało się połączyć z bazą danych '{NAZWA_POLACZENIA_DB}'. Sprawdź 'Secrets' w Ustawieniach.")
        st.stop() 

    # --- ZAKŁADKA 1: RAPORT GŁÓWNY (POPRAWIONA) ---
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

                dane_z_bazy = pobierz_dane_z_bazy(conn, data_start_rap, data_stop_rap)
                
                if dane_z_bazy.empty:
                    st.warning(f"Brak danych paliwowych w wybranym zakresie dat ({data_start_rap} - {data_stop_rap}).")
                else:
                    kurs_eur = pobierz_kurs_eur_pln()
                    if kurs_eur:
                        unikalne_waluty = dane_z_bazy['waluta'].unique()
                        mapa_kursow = pobierz_wszystkie_kursy(unikalne_waluty, kurs_eur)
                        
                        dane_z_bazy['data_transakcji_dt'] = pd.to_datetime(dane_z_bazy['data_transakcji'])
                        
                        # --- POPRAWKA BYŁA POTRZEBNA TUTAJ ---
                        dane_z_bazy['identyfikator_clean'] = dane_z_bazy['identyfikator'].astype(str).str.extract(r'([A-Z0-9]{4,})').str.upper().str.strip()
                        # --- KONIEC POPRAWKI ---

                        dane_z_bazy['identyfikator_clean'] = dane_z_bazy['identyfikator_clean'].fillna('Brak Identyfikatora')
                        dane_z_bazy['kwota_brutto_num'] = pd.to_numeric(dane_z_bazy['kwota_brutto'], errors='coerce').fillna(0.0)
                        dane_z_bazy['kurs_do_eur'] = dane_z_bazy['waluta'].map(mapa_kursow).fillna(0.0)
                        dane_z_bazy['kwota_finalna_eur'] = dane_z_bazy['kwota_brutto_num'] * dane_z_bazy['kurs_do_eur']
                        
                        st.subheader("Podsumowanie dla wybranego okresu")
                        podsumowanie = dane_z_bazy.groupby('identyfikator_clean')['kwota_finalna_eur'].sum().sort_values(ascending=False)
                        df_wynik = pd.DataFrame(podsumowanie)
                        df_wynik.rename(columns={'kwota_finalna_eur': 'Łączne wydatki (EUR)'}, inplace=True)
                        df_wynik.index.name = 'Identyfikator (Pojazd / Karta)'
                        
                        suma_laczna = df_wynik['Łączne wydatki (EUR)'].sum()
                        st.metric(label="SUMA ŁĄCZNA (Paliwo/Opłaty)", value=f"{suma_laczna:,.2f} EUR")
                        st.dataframe(df_wynik.style.format("{:,.2f} EUR", subset=['Łączne wydatki (EUR)']), use_container_width=True)
                        
                        st.divider() 
                        st.subheader("Szczegóły transakcji (Drill-down)")
                        
                        lista_pojazdow = ["--- Wybierz pojazd z listy ---"] + list(podsumowanie.index)
                        wybrany_pojazd = st.selectbox("Wybierz identyfikator, aby zobaczyć szczegóły:", lista_pojazdow)
                        
                        if wybrany_pojazd != "--- Wybierz pojazd z listy ---":
                            df_szczegoly = dane_z_bazy[dane_z_bazy['identyfikator_clean'] == wybrany_pojazd]
                            df_szczegoly = df_szczegoly.sort_values(by='data_transakcji_dt', ascending=False)
                            
                            df_szczegoly_display = df_szczegoly[['data_transakcji_dt', 'kwota_finalna_eur', 'kwota_brutto', 'waluta', 'zrodlo']]
                            df_szczegoly_display = df_szczegoly_display.rename(columns={
                                'data_transakcji_dt': 'Data transakcji',
                                'kwota_finalna_eur': 'Kwota (EUR)',
                                'kwota_brutto': 'Kwota oryginalna',
                                'waluta': 'Waluta',
                                'zrodlo': 'System'
                            })
                            
                            st.dataframe(
                                df_szczegoly_display,
                                use_container_width=True,
                                hide_index=True, 
                                column_config={
                                    "Data transakcji": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm"),
                                    "Kwota (EUR)": st.column_config.NumberColumn(format="%.2f EUR"),
                                    "Kwota oryginalna": st.column_config.NumberColumn(format="%.2f"),
                                }
                            )

        except Exception as e:
            if "does not exist" in str(e):
                 st.warning("Baza danych jest pusta lub nie została jeszcze utworzona. Przejdź do 'Panelu Admina', aby ją zainicjować.")
            else:
                 st.error(f"Wystąpił nieoczekiwany błąd w zakładce raportu: {e}")

    # --- ZAKŁADKA 2: RENTOWNOŚĆ (POPRAWIONA) ---
    with tab_rentownosc:
        st.header("Raport Rentowności (Zysk/Strata)")
        
        try:
            min_max_date_query = f"SELECT MIN(data_transakcji::date), MAX(data_transakcji::date) FROM {NAZWA_SCHEMATU}.{NAZWA_TABELI}"
            min_max_date = conn.query(min_max_date_query)
            if min_max_date.empty or min_max_date.iloc[0, 0] is None:
                st.info("Baza danych paliwowych jest pusta. Przejdź do Panelu Admina, aby wgrać pliki.")
                st.stop()
            domyslny_start_rent = min_max_date.iloc[0, 0]
            domyslny_stop_rent = min_max_date.iloc[0, 1]
        except Exception:
            st.info("Baza danych paliwowych jest pusta. Przejdź do Panelu Admina, aby wgrać pliki.")
            st.stop()
            
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
                    # KROK A: Pobierz koszty paliwa z bazy
                    dane_z_bazy = pobierz_dane_z_bazy(conn, data_start_rent, data_stop_rent)
                    
                    if dane_z_bazy.empty:
                        st.error("Brak danych paliwowych w wybranym okresie.")
                        st.session_state['raport_gotowy'] = False
                        st.stop()
                        
                    kurs_eur = pobierz_kurs_eur_pln()
                    if not kurs_eur: st.session_state['raport_gotowy'] = False; st.stop()
                    
                    unikalne_waluty = dane_z_bazy['waluta'].unique()
                    mapa_kursow = pobierz_wszystkie_kursy(unikalne_waluty, kurs_eur)
                    
                    # --- POPRAWKA BYŁA POTRZEBNA TUTAJ ---
                    dane_z_bazy['identyfikator_clean'] = dane_z_bazy['identyfikator'].astype(str).str.extract(r'([A-Z0-9]{4,})').str.upper().str.strip()
                    # --- KONIEC POPRAWKI ---
                    
                    dane_z_bazy['kwota_brutto_num'] = pd.to_numeric(dane_z_bazy['kwota_brutto'], errors='coerce').fillna(0.0)
                    dane_z_bazy['kurs_do_eur'] = dane_z_bazy['waluta'].map(mapa_kursow).fillna(0.0)
                    dane_z_bazy['kwota_finalna_eur'] = dane_z_bazy['kwota_brutto_num'] * dane_z_bazy['kurs_do_eur']
                    
                    df_koszty_paliwa = dane_z_bazy.groupby('identyfikator_clean')['kwota_finalna_eur'].sum().to_frame('Koszty Paliwa/Opłat (z Bazy)')

                    # KROK B: Przetwórz plik 'analiza.xlsx'
                    df_analiza = przetworz_plik_analizy(plik_analizy)
                    
                    if df_analiza is not None:
                        # KROK C: Połącz oba źródła danych
                        df_rentownosc = df_analiza.merge(
                            df_koszty_paliwa, 
                            left_index=True, 
                            right_index=True, 
                            how='outer'
                        )
                        
                        df_rentownosc = df_rentownosc.fillna(0)
                        
                        # KROK D: Oblicz zysk
                        df_rentownosc['ZYSK / STRATA (EUR)'] = (
                            df_rentownosc['przychody'] - 
                            df_rentownosc['koszty_inne'] - 
                            df_rentownosc['Koszty Paliwa/Opłat (z Bazy)']
                        )
                        
                        st.session_state['raport_gotowy'] = True
                        st.session_state['df_rentownosc'] = df_rentownosc
                        st.session_state['wybrany_pojazd_rent'] = "--- Wybierz pojazd ---" 
                        
        # --- BLOK WYŚWIETLANIA (BEZ ZMIAN) ---
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
                    koszty_paliwa = dane_pojazdu['Koszty Paliwa/Opłat (z Bazy)']
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
                'Koszty Paliwa/Opłat (z Bazy)',
                'ZYSK / STRATA (EUR)'
            ]].rename(columns={
                'przychody': 'Przychód (Subiekt)',
                'koszty_inne': 'Koszty Inne (Subiekt)'
            })
            
            st.dataframe(
                df_rentownosc_display.style.format("{:,.2f} EUR"),
                use_container_width=True
            )

    # --- ZAKŁADKA 3: PANEL ADMINA (BEZ ZMIAN) ---
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
            if wpisane_haswlo == prawidlowe_haslo:
                st.session_state["password_correct"] = True
                st.rerun() 
            else:
                st.error("Nieprawidłowe hasło.")
    return False

# --- GŁÓWNE URUCHOMIENIE APLIKACJI ---
if check_password():
    main_app()
