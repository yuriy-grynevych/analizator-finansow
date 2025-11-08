import pandas as pd
import numpy as np
import requests
import re
import streamlit as st # Zamiast tkinter
import time
from datetime import date

# --- USTAWIENIA STRONY ---
# To jest pierwsza komenda Streamlit - konfiguruje tytuł w zakładce przeglądarki
st.set_page_config(page_title="Analizator Wydatków", layout="wide")


# --- FUNKCJE NBP (BEZ ZMIAN) ---
# Używamy @st.cache_data, aby Streamlit pobrał kursy tylko raz
# i trzymał je w pamięci, nawet jeśli zmienimy daty.
@st.cache_data
def pobierz_kurs_eur_pln():
    try:
        url = 'http://api.nbp.pl/api/exchangerates/rates/a/eur/?format=json'
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        kurs = response.json()['rates'][0]['mid']
        print(f"Pobrano główny kurs NBP: 1 EUR = {kurs} PLN")
        return kurs
    except requests.exceptions.RequestException as e:
        print(f"Błąd pobierania kursu EUR/PLN: {e}")
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
            print(f"   -> Pobrano kurs dla {waluta_kod.upper()} (Tabela {tabela.upper()}): 1 {waluta_kod.upper()} = {kurs} PLN")
            return kurs
        except requests.exceptions.RequestException: pass
    print(f"   -> OSTRZEŻENIE: Nie znaleziono kursu NBP dla {waluta_kod.upper()}.")
    return None

@st.cache_data
def pobierz_wszystkie_kursy(waluty_lista, kurs_eur_pln):
    mapa_kursow_do_eur = {'EUR': 1.0, 'PLN': 1.0 / kurs_eur_pln}
    
    # Pasek postępu w Streamlit
    progress_bar = st.progress(0, text="Pobieranie kursów walut...")
    waluty_do_pobrania = [w for w in waluty_lista if w not in mapa_kursow_do_eur and pd.notna(w)]
    total_waluty = len(waluty_do_pobrania)
    
    for i, waluta in enumerate(waluty_do_pobrania):
        time.sleep(0.1) 
        kurs_pln = pobierz_kurs_do_pln(waluta)
        if kurs_pln: mapa_kursow_do_eur[waluta] = kurs_pln / kurs_eur_pln
        else: mapa_kursow_do_eur[waluta] = 0.0
        
        # Aktualizuj pasek postępu
        if total_waluty > 0:
            progress_bar.progress((i + 1) / total_waluty, text=f"Pobieranie kursu dla: {waluta}")
            
    progress_bar.empty() # Ukryj pasek po zakończeniu
    print("...Wszystkie kursy pobrane.")
    return mapa_kursow_do_eur

# --- FUNKCJE "TŁUMACZENIA" (BEZ ZMIAN) ---

def normalizuj_eurowag(df_eurowag):
    print(" -> Wykryto format Eurowag...")
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_eurowag['Data i godzina'], errors='coerce')
    df_out['identyfikator'] = df_eurowag['Tablica rejestracyjna'].fillna(df_eurowag['Karta'])
    df_out['kwota_brutto'] = pd.to_numeric(df_eurowag['Kwota brutto'], errors='coerce')
    df_out['waluta'] = df_eurowag['Waluta']
    df_out['zrodlo'] = 'Eurowag'
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

def normalizuj_e100(df_e100):
    print(" -> Wykryto format E100 (XLSX)...")
    df_out = pd.DataFrame()
    df_out['data_transakcji'] = pd.to_datetime(df_e100['Data'] + ' ' + df_e100['Czas'], format='%d.%m.%Y %H:%M:%S', errors='coerce')
    df_out['identyfikator'] = df_e100['Numer samochodu'].fillna(df_e100['Numer karty'])
    df_out['kwota_brutto'] = pd.to_numeric(df_e100['Kwota'], errors='coerce')
    df_out['waluta'] = df_e100['Waluta']
    df_out['zrodlo'] = 'E100'
    df_out = df_out.dropna(subset=['data_transakcji', 'kwota_brutto'])
    return df_out

# --- FUNKCJA WCZYTYWANIA (teraz czyta pliki z UPLOADERA) ---
@st.cache_data # Cache'ujemy też wczytywanie plików
def wczytaj_i_zunifikuj_pliki(przeslane_pliki):
    
    if not przeslane_pliki:
        return None, "Nie przesłano żadnych plików."
    
    print(f"Znaleziono {len(przeslane_pliki)} plików do przetworzenia:")
    
    lista_df_zunifikowanych = []
    
    for plik in przeslane_pliki:
        print(f" - Przetwarzam: {plik.name}")
        
        try:
            if plik.name.endswith('.csv'):
                # Logika dla CSV (na razie tylko stary E100)
                df = pd.read_csv(plik, sep=None, engine='python', on_bad_lines='skip')
                if df.shape[1] == 1: df = pd.read_csv(plik, sep=';', on_bad_lines='skip')
                kolumny = df.columns
                if 'ID Transakcji' in kolumny and 'Numer karty' in kolumny:
                     print(" -> Wykryto format E100 (CSV)... (POMIJAM)")
                     # lista_df_zunifikowanych.append(normalizuj_e100_csv(df))
                else:
                    print(f"   -> OSTRZEŻENIE: Pominięto CSV {plik.name}. Nie rozpoznano formatu.")
                    
            elif plik.name.endswith(('.xls', '.xlsx')):
                # Logika dla Excel
                df_pierwszy_arkusz = pd.read_excel(plik, engine='openpyxl')
                kolumny_pierwszego = df_pierwszy_arkusz.columns
                
                if 'Data i godzina' in kolumny_pierwszego and 'Artykuł' in kolumny_pierwszego:
                    lista_df_zunifikowanych.append(normalizuj_eurowag(df_pierwszy_arkusz))
                else:
                    print(f"   -> Pierwszy arkusz to nie Eurowag. Próbuję wczytać arkusz 'Transactions'...")
                    try:
                        df_arkusz_e100 = pd.read_excel(plik, sheet_name='Transactions', engine='openpyxl')
                        kolumny_e100 = df_arkusz_e100.columns
                        if 'Numer samochodu' in kolumny_e100 and 'Numer karty' in kolumny_e100:
                            lista_df_zunifikowanych.append(normalizuj_e100(df_arkusz_e100))
                        else:
                            print(f"   -> OSTRZEŻENIE: Pominięto plik {plik.name}. Znaleziono arkusz 'Transactions', ale nie pasuje do formatu E100.")
                    except Exception as e:
                        print(f"   -> OSTRZEŻENIE: Pominięto plik {plik.name}. Nie rozpoznano Eurowag i nie znaleziono arkusza 'Transactions'. Błąd: {e}")
            
        except Exception as e:
             print(f"   -> BŁĄD wczytania pliku {plik.name}: {e}")
             st.warning(f"Nie udało się wczytać pliku: {plik.name}. Powód: {e}")

    if not lista_df_zunifikowanych:
        return None, "Wczytano pliki, ale żaden nie pasował do formatu Eurowag ani E100."

    polaczone_df = pd.concat(lista_df_zunifikowanych, ignore_index=True)
    print(f"Połączono. Całkowita liczba transakcji: {len(polaczone_df)}")
    
    return polaczone_df, None

# --- FUNKCJA PRZETWARZANIA (BEZ ZMIAN) ---
def przetworz_dane(df_oryginal, mapa_kursow, data_start, data_stop):
    df = df_oryginal.copy()
    
    # Filtrowanie po dacie
    try:
        # Streamlitowe 'date_input' zwraca obiekty 'date', więc musimy je przekonwertować
        if data_start:
            df = df[df['data_transakcji'] >= pd.to_datetime(data_start)]
        if data_stop:
            # Dodajemy +1 dzień, aby objąć całą dobę 'data_stop'
            df = df[df['data_transakcji'] < (pd.to_datetime(data_stop) + pd.Timedelta(days=1))]
    except Exception as e:
        return None, f"Błędny format daty. Błąd: {e}"

    if df.empty:
        return None, f"Brak danych w wybranym zakresie dat ({data_start} - {data_stop})."

    # Czyszczenie i przeliczanie walut
    df['identyfikator_clean'] = df['identyfikator'].astype(str).str.extract(r'([A-Z0-9]{4,})')
    df['identyfikator_clean'] = df['identyfikator_clean'].fillna('Brak Identyfikatora')
    df['kwota_brutto_num'] = pd.to_numeric(df['kwota_brutto'], errors='coerce').fillna(0.0)
    df['kurs_do_eur'] = df['waluta'].map(mapa_kursow).fillna(0.0)
    df['kwota_finalna_eur'] = df['kwota_brutto_num'] * df['kurs_do_eur']
    
    return df, None

# --- GŁÓWNA CZĘŚĆ APLIKACJI STREAMLIT ---

st.title("Analizator Wydatków Floty (Eurowag + E100)")
st.subheader("Wszystkie waluty zostaną automatycznie przeliczone na EUR 💶")

# --- KROK 1: Przesyłanie plików ---
st.header("Krok 1: Wgraj pliki")
przeslane_pliki = st.file_uploader(
    "Wybierz lub przeciągnij pliki Eurowag (.xlsx) i E100 (.xlsx)",
    accept_multiple_files=True,
    type=['xlsx', 'xls', 'csv'] # Akceptujemy też CSV
)

if przeslane_pliki:
    # Wczytaj i zunifikuj dane
    dane_bazowe, blad_wczytania = wczytaj_i_zunifikuj_pliki(przeslane_pliki)
    
    if blad_wczytania:
        st.error(f"Błąd wczytywania plików: {blad_wczytania}")
    else:
        st.success(f"Poprawnie wczytano i połączono dane! Znaleziono {len(dane_bazowe)} transakcji.")
        
        # --- KROK 2: Pobieranie kursów NBP ---
        st.header("Krok 2: Pobieranie kursów")
        
        kurs_eur = pobierz_kurs_eur_pln()
        if kurs_eur:
            unikalne_waluty = dane_bazowe['waluta'].unique()
            mapa_kursow = pobierz_wszystkie_kursy(unikalne_waluty, kurs_eur)
            
            st.info(f"Pobrano kursy dla: {list(mapa_kursow.keys())}")
            
            # --- KROK 3: Filtry dat ---
            st.header("Krok 3: Generuj raport")
            
            # Domyślne daty
            domyslny_start = dane_bazowe['data_transakcji'].min().date()
            domyslny_stop = dane_bazowe['data_transakcji'].max().date()
            
            # Filtry dat w dwóch kolumnach
            col1, col2 = st.columns(2)
            with col1:
                data_start = st.date_input("Data Start", value=domyslny_start, min_value=domyslny_start, max_value=domyslny_stop)
            with col2:
                data_stop = st.date_input("Data Stop", value=domyslny_stop, min_value=domyslny_start, max_value=domyslny_stop)

            # Przycisk do generowania raportu
            if st.button("Generuj raport", type="primary"):
                
                # Przetwórz dane
                dane_przetworzone, blad_przetwarzania = przetworz_dane(dane_bazowe, mapa_kursow, data_start, data_stop)
                
                if blad_przetwarzania:
                    st.warning(blad_przetwarzania)
                else:
                    # --- KROK 4: Wyniki ---
                    st.header(f"Podsumowanie wydatków od {data_start} do {data_stop}")
                    
                    podsumowanie = dane_przetworzone.groupby('identyfikator_clean')['kwota_finalna_eur'].sum().sort_values(ascending=False)
                    
                    # Przygotowanie DataFrame do wyświetlenia
                    df_wynik = pd.DataFrame(podsumowanie)
                    df_wynik.rename(columns={'kwota_finalna_eur': 'Łączne wydatki (EUR)'}, inplace=True)
                    df_wynik.index.name = 'Identyfikator (Pojazd / Karta)'
                    
                    # SUMA ŁĄCZNA
                    suma_laczna = df_wynik['Łączne wydatki (EUR)'].sum()
                    
                    # Wyświetlenie sumy w ładnym boksie
                    st.metric(label="SUMA ŁĄCZNA (dla wybranego okresu)", value=f"{suma_laczna:,.2f} EUR")
                    
                    # Wyświetlenie tabeli z formatowaniem
                    st.dataframe(df_wynik.style.format("{:,.2f} EUR", subset=['Łączne wydatki (EUR)']), use_container_width=True)
else:
    st.info("Oczekuję na przesłanie plików...")