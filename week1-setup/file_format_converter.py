"""
File Format Converter and Performance Benchmarker
Compares CSV, JSON, and Parquet formats

Author: Jason Amar
Date: 2025-10-20
"""

import pandas as pd
import time
import os
from pathlib import Path

class FileFormatConverter:
    """
    Handles reading, writing, and converting between different file formats
    Also benchmarks performance metrics
    """
    
    def __init__(self, base_path='test_data'):
        """
        Initialize converter with base data path
        
        Args:
            base_path: Directory containing data files
        """
        self.base_path = Path(base_path)
        self.results = []
    
    def get_file_size(self, filepath):
        """
        Get file size in MB
        
        Args:
            filepath: Path to file
        
        Returns:
            File size in MB
        """
        size_bytes = os.path.getsize(filepath)
        size_mb = size_bytes / (1024 * 1024)
        return size_mb
    
    def read_csv(self, filename='sample_data.csv'):
        """
        Read CSV file and measure performance
        
        Returns:
            tuple: (DataFrame, read_time_seconds)
        """
        filepath = self.base_path / filename
        print(f"\n📄 Reading CSV: {filepath}")
        
        start_time = time.time()
        df = pd.read_csv(filepath)
        read_time = time.time() - start_time
        
        file_size = self.get_file_size(filepath)
        
        print(f"   ✅ Read {len(df):,} rows in {read_time:.4f} seconds")
        print(f"   📦 File size: {file_size:.2f} MB")
        
        self.results.append({
            'format': 'CSV',
            'operation': 'read',
            'time_seconds': read_time,
            'file_size_mb': file_size,
            'rows': len(df)
        })
        
        return df, read_time
    
    def read_json(self, filename='sample_data.json'):
        """
        Read JSON file and measure performance
        
        Returns:
            tuple: (DataFrame, read_time_seconds)
        """
        filepath = self.base_path / filename
        print(f"\n📄 Reading JSON: {filepath}")
        
        start_time = time.time()
        df = pd.read_json(filepath)
        read_time = time.time() - start_time
        
        file_size = self.get_file_size(filepath)
        
        print(f"   ✅ Read {len(df):,} rows in {read_time:.4f} seconds")
        print(f"   📦 File size: {file_size:.2f} MB")
        
        self.results.append({
            'format': 'JSON',
            'operation': 'read',
            'time_seconds': read_time,
            'file_size_mb': file_size,
            'rows': len(df)
        })
        
        return df, read_time
    
    def read_parquet(self, filename='sample_data.parquet'):
        """
        Read Parquet file and measure performance
        
        Returns:
            tuple: (DataFrame, read_time_seconds)
        """
        filepath = self.base_path / filename
        print(f"\n📄 Reading Parquet: {filepath}")
        
        start_time = time.time()
        df = pd.read_parquet(filepath, engine='pyarrow')
        read_time = time.time() - start_time
        
        file_size = self.get_file_size(filepath)
        
        print(f"   ✅ Read {len(df):,} rows in {read_time:.4f} seconds")
        print(f"   📦 File size: {file_size:.2f} MB")
        
        self.results.append({
            'format': 'Parquet',
            'operation': 'read',
            'time_seconds': read_time,
            'file_size_mb': file_size,
            'rows': len(df)
        })
        
        return df, read_time
    
    def write_csv(self, df, filename='output.csv'):
        """Write DataFrame to CSV and measure performance"""
        filepath = self.base_path / filename
        print(f"\n💾 Writing CSV: {filepath}")
        
        start_time = time.time()
        df.to_csv(filepath, index=False)
        write_time = time.time() - start_time
        
        file_size = self.get_file_size(filepath)
        
        print(f"   ✅ Wrote {len(df):,} rows in {write_time:.4f} seconds")
        print(f"   📦 File size: {file_size:.2f} MB")
        
        self.results.append({
            'format': 'CSV',
            'operation': 'write',
            'time_seconds': write_time,
            'file_size_mb': file_size,
            'rows': len(df)
        })
        
        return write_time
    
    def write_json(self, df, filename='output.json'):
        """Write DataFrame to JSON and measure performance"""
        filepath = self.base_path / filename
        print(f"\n💾 Writing JSON: {filepath}")
        
        start_time = time.time()
        df.to_json(filepath, orient='records', date_format='iso')
        write_time = time.time() - start_time
        
        file_size = self.get_file_size(filepath)
        
        print(f"   ✅ Wrote {len(df):,} rows in {write_time:.4f} seconds")
        print(f"   📦 File size: {file_size:.2f} MB")
        
        self.results.append({
            'format': 'JSON',
            'operation': 'write',
            'time_seconds': write_time,
            'file_size_mb': file_size,
            'rows': len(df)
        })
        
        return write_time
    
    def write_parquet(self, df, filename='output.parquet'):
        """Write DataFrame to Parquet and measure performance"""
        filepath = self.base_path / filename
        print(f"\n💾 Writing Parquet: {filepath}")
        
        start_time = time.time()
        df.to_parquet(filepath, index=False, engine='pyarrow', compression='snappy')
        write_time = time.time() - start_time
        
        file_size = self.get_file_size(filepath)
        
        print(f"   ✅ Wrote {len(df):,} rows in {write_time:.4f} seconds")
        print(f"   📦 File size: {file_size:.2f} MB")
        
        self.results.append({
            'format': 'Parquet',
            'operation': 'write',
            'time_seconds': write_time,
            'file_size_mb': file_size,
            'rows': len(df)
        })
        
        return write_time
    
    def generate_performance_report(self):
        """
        Generate comprehensive performance comparison report
        """
        if not self.results:
            print("No benchmark results available!")
            return
        
        results_df = pd.DataFrame(self.results)
        
        print("\n" + "="*70)
        print("PERFORMANCE BENCHMARK RESULTS")
        print("="*70)
        
        # Group by format and operation
        for operation in ['read', 'write']:
            op_data = results_df[results_df['operation'] == operation]
            
            print(f"\n{operation.upper()} Performance:")
            print("-" * 70)
            
            for _, row in op_data.iterrows():
                print(f"{row['format']:10} | "
                      f"Time: {row['time_seconds']:.4f}s | "
                      f"Size: {row['file_size_mb']:.2f} MB")
        
        # Find winners
        read_data = results_df[results_df['operation'] == 'read']
        write_data = results_df[results_df['operation'] == 'write']
        
        fastest_read = read_data.loc[read_data['time_seconds'].idxmin()]
        fastest_write = write_data.loc[write_data['time_seconds'].idxmin()]
        smallest_file = results_df.loc[results_df['file_size_mb'].idxmin()]
        
        print("\n" + "="*70)
        print("WINNERS")
        print("="*70)
        print(f"🏆 Fastest Read:  {fastest_read['format']} ({fastest_read['time_seconds']:.4f}s)")
        print(f"🏆 Fastest Write: {fastest_write['format']} ({fastest_write['time_seconds']:.4f}s)")
        print(f"🏆 Smallest File: {smallest_file['format']} ({smallest_file['file_size_mb']:.2f} MB)")
        
        # Save results to file
        report_path = 'performance_results.txt'
        with open(report_path, 'w') as f:
            f.write("FILE FORMAT PERFORMANCE COMPARISON\n")
            f.write("="*70 + "\n\n")
            f.write(results_df.to_string())
            f.write("\n\n" + "="*70 + "\n")
            f.write(f"Fastest Read:  {fastest_read['format']}\n")
            f.write(f"Fastest Write: {fastest_write['format']}\n")
            f.write(f"Smallest File: {smallest_file['format']}\n")
        
        print(f"\n📊 Full report saved to: {report_path}")

def main():
    """
    Main function to run all benchmarks
    """
    print("="*70)
    print("FILE FORMAT CONVERTER & PERFORMANCE BENCHMARKER")
    print("="*70)
    
    # Initialize converter
    converter = FileFormatConverter()
    
    # Read all formats
    print("\n--- READING FILES ---")
    df_csv, _ = converter.read_csv()
    df_json, _ = converter.read_json()
    df_parquet, _ = converter.read_parquet()
    
    # Verify all DataFrames are identical
    print("\n--- VERIFYING DATA INTEGRITY ---")
    if df_csv.shape == df_json.shape == df_parquet.shape:
        print("✅ All formats contain same number of rows and columns")
    else:
        print("⚠️  Warning: Formats have different shapes!")
    
    # Write all formats (using CSV data as source)
    print("\n--- WRITING FILES ---")
    converter.write_csv(df_csv, 'output.csv')
    converter.write_json(df_csv, 'output.json')
    converter.write_parquet(df_csv, 'output.parquet')
    
    # Generate performance report
    converter.generate_performance_report()
    
    print("\n" + "="*70)
    print("✅ BENCHMARK COMPLETE!")
    print("="*70)

if __name__ == "__main__":
    main()