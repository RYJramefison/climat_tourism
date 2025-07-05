import pandas as pd
import os

def combine_historical_and_recent(city, data_dir="airflow/dags/climat_tourisme/data"):
    """
    Combine historical and recent weather data for a given city.

    Parameters:
    - city (str): Name of the city
    - data_dir (str): Path to the directory where the CSVs are located

    Output:
    - Saves a combined CSV file named 'weather_combined_<city>.csv'
    """
    hist_path = os.path.join(data_dir, f"weather_historical_{city}.csv")
    recent_path = os.path.join(data_dir, f"weather_{city}.csv")
    output_path = os.path.join(data_dir, f"weather_combined_{city}.csv")

    if not os.path.exists(hist_path):
        print(f"[!] Missing historical file for {city}: {hist_path}")
        return

    if not os.path.exists(recent_path):
        print(f"[!] Missing recent file for {city}: {recent_path}")
        return

    try:
        df_hist = pd.read_csv(hist_path)
        df_recent = pd.read_csv(recent_path)

        # Assure cohérence des colonnes et formats
        for df in [df_hist, df_recent]:
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"])
            else:
                raise ValueError("Missing 'Date' column in one of the files.")

        # Combine les deux fichiers sans doublons
        df_combined = pd.concat([df_hist, df_recent], ignore_index=True)
        df_combined = df_combined.sort_values("Date").drop_duplicates(subset="Date")

        for col in ["Temperature", "Wind_Speed", "Rain", "Score"]:
            if col in df_combined.columns:
                df_combined[col] = df_combined[col].round(0).astype(int)
        
        df_combined.to_csv(output_path, index=False)
        print(f"[✓] Saved combined data: {output_path}")

    except Exception as e:
        print(f"[X] Error while combining data for {city}: {e}")
