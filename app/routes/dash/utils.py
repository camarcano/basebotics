import os
import pandas as pd
import duckdb
from datetime import datetime
import sqlite3
import numpy as np
import gdown
from concurrent.futures import ThreadPoolExecutor
import stat
import shutil

class DHHCalculator:
    def __init__(self, parquet_file, google_sheet_url, db_file_name, table_name, output_file, base_dir):
        """
        Initialize the DHHCalculator with the necessary file paths and URLs.
        Ensure the base directory exists and files inherit proper permissions.
        """
        self.base_dir = base_dir
        self.parquet_file = os.path.join(base_dir, parquet_file)
        self.google_sheet_url = google_sheet_url
        self.db_file_name = os.path.join(base_dir, db_file_name)
        self.table_name = table_name
        self.output_file = os.path.join(base_dir, output_file)

        # Ensure base directory exists
        os.makedirs(base_dir, exist_ok=True)
        
        # Set directory permissions
        self.set_permissions(base_dir)

        # Pre-compile the DHH threshold calculation
        self.calculate_dhh_threshold = np.vectorize(
            lambda x: -0.0049 * x**2 + 0.0853 * x + 105.05
        )

    def set_permissions(self, path):
        """
        Ensure proper permissions and ownership of a given path.
        """
        try:
            # Inherit permissions and ownership from the parent directory
            base_stat = os.stat(path)
            os.chmod(path, base_stat.st_mode | stat.S_IWUSR | stat.S_IWGRP | stat.S_IROTH)
            shutil.chown(path, group=base_stat.st_gid)
        except Exception as e:
            print(f"Error setting permissions for {path}: {e}")

    def load_data(self):
        """
        Load and filter parquet data using DuckDB.
        """
        required_columns = [
            'game_date', 'player_name', 'batter', 'description', 
            'launch_speed', 'launch_angle', 'release_speed', 
            'hit_distance_sc'
        ]
        con = duckdb.connect(database=':memory:')
        try:
            query = f"""
                SELECT {', '.join(required_columns)}
                FROM parquet_scan('{self.parquet_file}')
                WHERE description != 'foul'
            """
            df = con.execute(query).df()
            print(f"Loaded data shape: {df.shape}")
        finally:
            con.close()
        return df

    def calculate_missing_columns(self, df):
        """
        Optimized column calculation using vectorized operations.
        """
        # Convert to numeric once and drop NaN values
        numeric_cols = ['launch_speed', 'launch_angle']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        df = df.dropna(subset=numeric_cols)
        
        # Pre-calculate common expressions
        ls = df['launch_speed']
        la = df['launch_angle']
        ls_mult_1_5_minus_la = ls * 1.5 - la
        ls_plus_la = ls + la
        ls_mult_2_minus_la = ls * 2 - la
        ls_plus_la_mult_2 = ls + la * 2
        
        # Initialize all columns at once
        new_columns = ['Barrel', 'Solid-Contact', 'Poorly-Weak', 'Flare-or-Burner', 
                      'Poorly-Under', 'Poorly-Topped', 'Unclassified']
        df[new_columns] = 0
        
        # Vectorized conditions for each classification
        barrel_mask = (
            (ls_mult_1_5_minus_la >= 117) & 
            (ls_plus_la >= 124) & 
            (ls >= 98) & 
            (la.between(4, 50))
        )
        df.loc[barrel_mask, 'Barrel'] = 1
        
        solid_contact_mask = (
            (ls_mult_1_5_minus_la >= 111) & 
            (ls_plus_la >= 119) & 
            (ls >= 95) & 
            (la.between(0, 52))
        )
        df.loc[solid_contact_mask, 'Solid-Contact'] = 1
        
        poorly_weak_mask = (ls <= 59)
        df.loc[poorly_weak_mask, 'Poorly-Weak'] = 1
        
        flare_burner_mask = (
            ((ls_mult_2_minus_la >= 87) & (la <= 41) & 
             (ls * 2 + la <= 175) & (ls + la * 1.3 >= 89) & 
             (ls.between(59, 72))) |
            ((ls + la * 1.3 <= 112) & (ls + la * 1.55 >= 92) & 
             (ls.between(72, 86))) |
            ((la <= 20) & (ls + la * 2.4 >= 98) & 
             (ls.between(86, 95))) |
            ((ls - la >= 76) & (ls + la * 2.4 >= 98) & 
             (ls >= 95) & (la <= 30))
        )
        df.loc[flare_burner_mask, 'Flare-or-Burner'] = 1
        
        poorly_under_mask = (ls_plus_la_mult_2 >= 116)
        df.loc[poorly_under_mask, 'Poorly-Under'] = 1
        
        poorly_topped_mask = (ls_plus_la_mult_2 <= 116)
        df.loc[poorly_topped_mask, 'Poorly-Topped'] = 1
        
        # Set Unclassified efficiently
        classification_columns = [col for col in new_columns if col != 'Unclassified']
        unclassified_mask = (df[classification_columns].sum(axis=1) == 0)
        df.loc[unclassified_mask, 'Unclassified'] = 1
        
        # Calculate DHH efficiently
        df['DHH_Thres'] = self.calculate_dhh_threshold(la)
        df['DHH'] = (ls > df['DHH_Thres']).astype(int)
        
        return df

    def filter_data(self, df, stdate, endate, min_ip):
        """
        Optimized data filtering with efficient date handling.
        """
        # Convert dates once
        df['game_date'] = pd.to_datetime(df['game_date'], errors='coerce')
        date_mask = (
            (df['game_date'] >= pd.to_datetime(stdate)) & 
            (df['game_date'] <= pd.to_datetime(endate))
        )
        df = df[date_mask]
        
        # Convert numeric columns efficiently
        numeric_cols = ['release_speed', 'hit_distance_sc']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        
        # Add BBE column
        df['BBE'] = 1
        
        return df

    def calculate_dhh(self, df):
        """
        Optimized DHH calculation using efficient groupby operations.
        """
        # Define aggregations
        agg_dict = {
            'BBE': 'sum',
            'Barrel': 'sum',
            'Solid-Contact': 'sum',
            'Poorly-Weak': 'sum',
            'Flare-or-Burner': 'sum',
            'Poorly-Under': 'sum',
            'Poorly-Topped': 'sum',
            'Unclassified': 'sum',
            'DHH': 'sum',
            'release_speed': 'mean',
            'hit_distance_sc': 'mean',
            'launch_speed': ['mean', 'max', 
                           lambda x: x.quantile(0.5),
                           lambda x: x.quantile(0.9),
                           lambda x: x.quantile(0.95)],
            'launch_angle': ['mean', 'std']
        }
        
        # Perform groupby efficiently
        grouped_df = df.groupby(['player_name', 'batter']).agg(agg_dict).reset_index()
        
        # Rename columns
        grouped_df.columns = ['player_name', 'batter', 'BBE', 'Barrel', 'Solid-Contact',
                            'Poorly-Weak', 'Flare-or-Burner', 'Poorly-Under', 'Poorly-Topped',
                            'Unclassified', 'DHH', 'AVG Pitches Velo', 'AVG Hit Distance',
                            'AVG EV', 'MaxEV', 'P50 EV', 'P90 EV', 'P95 EV', 'AVG LA', 'Sd(LA)']
        
        # Calculate percentages efficiently
        percentage_cols = ['Barrel', 'Solid-Contact', 'Poorly-Weak', 'Flare-or-Burner',
                         'Poorly-Under', 'Poorly-Topped', 'Unclassified', 'DHH']
        for col in percentage_cols:
            grouped_df[f'{col}%'] = grouped_df[col] / grouped_df['BBE'] * 100
            grouped_df.drop(columns=[col], inplace=True)
        
        # Reorder columns
        final_cols = ['player_name', 'batter', 'BBE', 'DHH%', 'Sd(LA)', 'AVG LA',
                     'Barrel%', 'MaxEV', 'P95 EV', 'P90 EV', 'P50 EV', 'AVG EV',
                     'AVG Pitches Velo', 'AVG Hit Distance', 'Solid-Contact%',
                     'Poorly-Weak%', 'Flare-or-Burner%', 'Poorly-Under%', 'Poorly-Topped%']
        grouped_df = grouped_df[final_cols]
        
        # Final column renaming
        grouped_df.rename(columns={'AVG LA': 'LA', 'AVG EV': 'EV'}, inplace=True)
        
        return grouped_df



    def merge_with_player_ids(self, df):
        """
        Merge data with player IDs from SQLite database.
        """
        # Create database if it doesn't exist
        if not os.path.exists(self.db_file_name):
            conn = sqlite3.connect(self.db_file_name)
            try:
                # Download Google sheet and save to CSV
                gdown.download(self.google_sheet_url, self.output_file, quiet=False)
                player_id_df = pd.read_csv(self.output_file)
                player_id_df.to_sql(self.table_name, conn, index=False)
            finally:
                conn.close()

            # Set permissions on the database file
            self.set_permissions(self.db_file_name)

        # Read from the database
        with sqlite3.connect(self.db_file_name) as conn:
            player_id_df = pd.read_sql(f"SELECT * FROM {self.table_name}", conn)

        # Merge and clean up efficiently
        df = pd.merge(df, player_id_df, how='left', left_on='batter', right_on='MLBID')
        df.drop(columns=['batter', 'MLBID'], inplace=True, errors='ignore')
        df.rename(columns={'FANGRAPHSNAME': 'Name'}, inplace=True)
        
        return df

    def filter_by_min_ip(self, df, min_ip):
        """
        Efficient minimum IP filtering.
        """
        return df[df['BBE'] >= min_ip]

    def save_to_csv(self, df):
        """
        Save the DataFrame to CSV and ensure proper permissions.
        """
        df.to_csv(self.output_file, index=False)
        self.set_permissions(self.output_file)

    def process(self, stdate, endate, min_ip):
        """
        Optimized main process with error handling and parallel processing.
        """
        try:
            with ThreadPoolExecutor() as executor:
                # Load data
                future_data = executor.submit(self.load_data)
                data = future_data.result()
                
                # Continue processing
                processed_data = self.calculate_missing_columns(data)
                filtered_data = self.filter_data(processed_data, stdate, endate, min_ip)
                dhh_data = self.calculate_dhh(filtered_data)
                merged_data = self.merge_with_player_ids(dhh_data)
                final_data = self.filter_by_min_ip(merged_data, min_ip)
                
                # Save results
                self.save_to_csv(final_data)
                
            return final_data
        except Exception as e:
            print(f"An error occurred during processing: {e}")
            raise
