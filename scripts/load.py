def save_to_csv(df, filename="data/weather_Paris.csv"):
    df.to_csv(filename, index=False)
