import math
import os
import time
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"]
SOURCE_BASE_URL = os.environ["AHON_PHIVOLCS_BASE_URL"].rstrip("/")


def scrape_current_month_from_main_page():
    """
    Scrapes the latest earthquake data from the main PHIVOLCS page.
    This is used for the current month that doesn't have a dedicated monthly page yet.
    """
    url = SOURCE_BASE_URL + "/"

    try:
        print("  Fetching from main page (current month)...", end=" ")

        response = requests.get(url, timeout=15)
        response.raise_for_status()

        tables = pd.read_html(StringIO(response.text), skiprows=1)

        df = None
        for table in tables:
            if table.shape[1] >= 5:
                df = table
                break

        if df is None or df.empty:
            print("✗ No data found")
            return None

        expected_columns = [
            'Date-Time',
            'Latitude',
            'Longitude',
            'Depth',
            'Magnitude',
            'Location'
        ]

        if df.shape[1] == 6:
            df.columns = expected_columns
        elif df.shape[1] > 6:
            df = df.iloc[:, :6]
            df.columns = expected_columns
        else:
            print(f"✗ Invalid columns ({df.shape[1]})")
            return None

        mask = (
            df['Date-Time'].astype(str).str.contains('Date|Time|Philippine', case=False, na=False) |
            df['Latitude'].astype(str).str.contains('Latitude|ºN|°N', case=False, na=False) |
            df['Longitude'].astype(str).str.contains('Longitude|ºE|°E', case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)

        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains('total|no. of events', na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r'^[A-Z][a-z]{2}-\d{2}$', na=False)
            df = df[~(summary_mask | month_abbrev_mask)]

        df = df.dropna(how='all').reset_index(drop=True)

        current_month = datetime.now(timezone.utc).strftime("%B")
        current_year = datetime.now(timezone.utc).year

        df['Month'] = current_month
        df['Year'] = current_year

        print(f"✓ {len(df)} records")

        return df

    except (requests.RequestException, pd.errors.ParserError) as e:
        print(f"✗ Error: {e}")
        return None


def month_is_current(year, month_name, now=None):
    """Identify the current UTC month for the PHIVOLCS main-page fallback."""
    current_time = now or datetime.now(timezone.utc)
    return year == current_time.year and month_name == current_time.strftime("%B")


def scrape_phivolcs_data_from_html(year, month_name):
    """
    Fetches earthquake data by reading the HTML table from the PHIVOLCS monthly page.
    The main-page fallback is limited to the current UTC month to avoid relabeling
    current observations as data for an earlier month.
    """
    url = (
        f"{SOURCE_BASE_URL}/EQLatest-Monthly/"
        f"{year}/{year}_{month_name}.html"
    )

    try:
        print(f"  Fetching: {month_name} {year}...", end=" ")

        response = requests.get(url, timeout=15)
        response.raise_for_status()

        tables = pd.read_html(StringIO(response.text), skiprows=1)

        df = None
        for table in tables:
            if table.shape[1] >= 5:
                df = table
                break

        if df is None or df.empty:
            print("✗ No data")
            return None

        expected_columns = [
            'Date-Time',
            'Latitude',
            'Longitude',
            'Depth',
            'Magnitude',
            'Location'
        ]

        if df.shape[1] == 6:
            df.columns = expected_columns
        elif df.shape[1] > 6:
            df = df.iloc[:, :6]
            df.columns = expected_columns
        else:
            print(f"✗ Invalid columns ({df.shape[1]})")
            return None

        mask = (
            df['Date-Time'].astype(str).str.contains('Date|Time|Philippine', case=False, na=False) |
            df['Latitude'].astype(str).str.contains('Latitude|ºN|°N', case=False, na=False) |
            df['Longitude'].astype(str).str.contains('Longitude|ºE|°E', case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)

        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains('total|no. of events', na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r'^[A-Z][a-z]{2}-\d{2}$', na=False)
            df = df[~(summary_mask | month_abbrev_mask)]

        df = df.dropna(how='all').reset_index(drop=True)

        df['Month'] = month_name
        df['Year'] = year

        print(f"✓ {len(df)} records")

        return df

    except requests.exceptions.HTTPError as errh:
        if errh.response.status_code == 404:
            if month_is_current(year, month_name):
                print("✗ HTTP 404 (trying main page)")
                return scrape_current_month_from_main_page()
            print("✗ HTTP 404")
            return None
        else:
            print(f"✗ HTTP {errh.response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"✗ Error: {e}")
        return None



def scrape_year_data(year, output_dir="data"):
    """
    Scrapes earthquake data for all months in a given year.
    Returns the combined DataFrame for that year.
    """
    print(f"\n{'─'*70}")
    print(f"📅 Scraping Year: {year}")
    print(f"{'─'*70}")
    
    all_data = []
    successful_months = []
    failed_months = []
    current_month_found = False
    
    for month_name in MONTH_NAMES:
        # Skip future months if we've already found the current month
        if current_month_found:
            print(f"  Skipping: {month_name} {year} (future month)")
            failed_months.append(month_name)
            continue
            
        df = scrape_phivolcs_data_from_html(year, month_name)
        
        if df is not None and not df.empty:
            all_data.append(df)
            successful_months.append(month_name)
            
            # Check if this data came from the main page (current month indicator)
            if month_is_current(year, month_name):
                current_month_found = True
                print(f"  ℹ️  Current month detected: {month_name} {year}")
        else:
            failed_months.append(month_name)
            if month_is_current(year, month_name):
                current_month_found = True
        
        # Be polite to the server
        time.sleep(0.5)
    
    # Combine and save data for this year
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to separate file for this year
        output_filename = os.path.join(output_dir, f"phivolcs_earthquake_{year}.csv")
        combined_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        
        print(f"\n✓ Year {year} Complete:")
        print(f"  • Total records: {len(combined_df)}")
        print(f"  • Successful months: {len(successful_months)}")
        print(f"  • File saved: {output_filename}")
        
        return combined_df
    else:
        print(f"\n✗ No data retrieved for {year}")
        return None


def scrape_multiple_years(years_back=3, output_dir="data"):
    """
    Scrapes earthquake data for the last N years.
    Each year is saved as a separate CSV file.
    """
    current_year = datetime.now(timezone.utc).year
    start_year = current_year - years_back + 1
    
    print(f"\n{'='*70}")
    print("🌏 PHIVOLCS EARTHQUAKE DATA SCRAPER")
    print(f"{'='*70}")
    print(f"📊 Scraping Range: {start_year} - {current_year}")
    print(f"📁 Output Directory: {output_dir}/")
    print(f"{'='*70}")
    
    all_years_data = []
    scrape_summary = {}
    
    # Scrape each year
    for year in range(start_year, current_year + 1):
        df = scrape_year_data(year, output_dir)
        
        if df is not None:
            all_years_data.append(df)
            scrape_summary[year] = len(df)
        else:
            scrape_summary[year] = 0
    
    # Create a combined file with all years
    if all_years_data:
        combined_all = pd.concat(all_years_data, ignore_index=True)
        combined_filename = os.path.join(output_dir, "phivolcs_earthquake_all_years.csv")
        combined_all.to_csv(combined_filename, index=False, encoding='utf-8-sig')
        
        # Print final summary
        print(f"\n{'='*70}")
        print("✅ SCRAPING COMPLETE!")
        print(f"{'='*70}")
        print("\n📊 Summary by Year:")
        for year, count in scrape_summary.items():
            print(f"  • {year}: {count:,} earthquakes")
        print(f"\n📈 Total Records: {len(combined_all):,}")
        print("\n📁 Files Created:")
        for year in range(start_year, current_year + 1):
            if scrape_summary.get(year, 0) > 0:
                print(f"  • {output_dir}/phivolcs_earthquake_{year}.csv")
        print(f"  • {output_dir}/phivolcs_earthquake_all_years.csv (combined)")
        print(f"\n{'='*70}\n")
        
        return combined_all, scrape_summary
    else:
        print("\n✗ No data was retrieved for any year.")
        return None, {}

# Summary statistics ng equake
def display_statistics(df):
    """
    Display basic statistics about the scraped data.
    """
    if df is None or df.empty:
        return
    
    print(f"{'='*70}")
    print("📈 DATA STATISTICS")
    print(f"{'='*70}\n")
    
    # Magnitude statistics
    print("🔢 Magnitude Statistics:")
    print(df['Magnitude'].describe())
    
    # Yearly breakdown
    print("\n📅 Earthquakes by Year:")
    yearly_counts = df.groupby('Year').size().sort_index()
    for year, count in yearly_counts.items():
        print(f"  • {year}: {count:,} earthquakes")
    
    ranked_magnitudes, invalid_magnitude_count = rank_magnitudes(df['Magnitude'])
    print(
        "Rows with missing or malformed magnitude omitted from ranking: "
        f"{invalid_magnitude_count:,}"
    )

    # Keep source values untouched; numeric conversion is only for this summary.
    print("\n💥 Top 10 Strongest Earthquakes:")
    for magnitude, idx in ranked_magnitudes[:10]:
        row = df.loc[idx]
        print(f"  • Mag {magnitude:g} - {row['Location'][:50]} ({row['Year']})")
    
    print(f"\n{'='*70}\n")


def rank_magnitudes(magnitudes):
    """Return numeric magnitude/index pairs and count values not rankable."""
    ranked = []
    invalid_count = 0
    for index, value in magnitudes.items():
        try:
            magnitude = float(value)
        except (TypeError, ValueError):
            invalid_count += 1
            continue
        if not math.isfinite(magnitude):
            invalid_count += 1
            continue
        ranked.append((magnitude, index))
    return sorted(ranked, key=lambda item: item[0], reverse=True), invalid_count


if __name__ == "__main__":
    # Configuration
    YEARS_TO_SCRAPE = int(os.environ["AHON_PHIVOLCS_YEARS_BACK"])

    #guys, change this if u want to reflect this on your catalog 
    catalog = os.environ.get("AHON_CATALOG", "ahon")
    source_volume = os.environ.get("AHON_SOURCE_VOLUME", f"/Volumes/{catalog}/reference/source")
    OUTPUT_DIR = os.path.join(source_volume, "philvolcs_earthquake")
    
    # Run the scraper
    combined_df, summary = scrape_multiple_years(
        years_back=YEARS_TO_SCRAPE,
        output_dir=OUTPUT_DIR
    )
    
    # Display statistics
    if combined_df is not None:
        display_statistics(combined_df)
