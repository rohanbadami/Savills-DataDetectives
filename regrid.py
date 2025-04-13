import time
import csv
import json
import os
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from tabulate import tabulate

df_leases = pd.read_csv("Leases.csv")
df_leases.columns = df_leases.columns.str.strip()

market_map = {
    'Austin': 'Austin',
    'Chicago': 'Chicago',
    'Chicago Suburbs': 'Chicago',
    'Dallas/Ft Worth': 'Dallas/Ft. Worth',
    'Houston': 'Houston',
    'Los Angeles': 'Los Angeles',
    'Philadelphia': 'Philadelphia',
    'San Francisco': 'San Francisco',
    'South Bay/San Jose': 'South Bay/San Jose',
}
df_leases = df_leases[df_leases['market'].isin(market_map.keys())]
df_leases['market_std'] = df_leases['market'].map(market_map)
df_leases['leasedSF'] = pd.to_numeric(df_leases['leasedSF'], errors='coerce')

df_classA = df_leases[df_leases['internal_class'] == 'A'].copy()
leased_sf_by_building = df_classA.groupby(
    'costarID')['leasedSF'].sum().reset_index()
leased_sf_filtered = leased_sf_by_building[leased_sf_by_building['leasedSF'] > 10000]

if 'building_name' in df_classA.columns:
    building_info = df_classA[['costarID', 'address', 'zip',
                               'market_std', 'CBD_suburban', 'building_name']].drop_duplicates()
    building_info = building_info[building_info['building_name'].notna() & (
        building_info['building_name'].str.strip() != "")]
else:
    building_info = df_classA[['costarID', 'address', 'zip',
                               'market_std', 'CBD_suburban']].drop_duplicates()

filtered_buildings = pd.merge(
    leased_sf_filtered, building_info, on='costarID', how='left')
# Clean up ZIP: remove trailing ".0"
filtered_buildings["zip"] = filtered_buildings["zip"].astype(
    str).str.replace(r'\.0$', '', regex=True)

print("CBD/Suburban Distribution:")
print(filtered_buildings['CBD_suburban'].value_counts(normalize=True))

# Define target sample size and sample proportionally.
target_total = 1900
suburban_frac = 0.608
cbd_n = target_total - int(target_total * suburban_frac)
suburban_n = int(target_total * suburban_frac)

cbd_subset = filtered_buildings[filtered_buildings['CBD_suburban'] == 'CBD']
suburban_subset = filtered_buildings[filtered_buildings['CBD_suburban'] == 'Suburban']

cbd_sample = cbd_subset.sample(n=min(cbd_n, len(cbd_subset)), random_state=42)
suburban_sample = suburban_subset.sample(
    n=min(suburban_n, len(suburban_subset)), random_state=42)

final_buildings = pd.concat(
    [cbd_sample, suburban_sample]).reset_index(drop=True)
print(f"\n✅ Final sample size: {len(final_buildings)} rows")
pd.set_option('display.max_rows', 10)
print(tabulate(final_buildings.head(10), headers='keys', tablefmt='psql'))


expected_fields = [
    "Parcel ID",
    "Parcel Address",
    "Parcel Address Zip Code",
    "Enhanced Owner",
    "Deeded Owner",
    "Total Parcel Value",
    "Improvement Value",
    "Land Value",
    "Assessed Value",
    "Total Appraised Value",
    "Prior Total Market Value",
    "Zoning Code",
    "Parcel Use Description",
    "Structure Year Built",
    "Year Remodeled",
    "Regrid Calculated Building Footprint Square Feet",
    "Total Square Footage of Structures",
    "Net Rent Area",
    "Year Improved",
    "USPS Vacancy Indicator",
    "USPS Vacancy Indicator Date",
    "Class Structure",
    "Quality"
]
output_filename = "property_data_for_all_buildings.csv"
write_header = not os.path.exists(output_filename)
final_fieldnames = ["costarID", "address", "zip", "query"] + expected_fields

if os.path.exists(output_filename):
    existing_results = pd.read_csv(output_filename)
    processed_ids = existing_results['costarID'].astype(str).unique().tolist()
    final_buildings = final_buildings[~final_buildings['costarID'].astype(
        str).isin(processed_ids)]
    print(
        f"Resuming... {len(final_buildings)} buildings remaining to process.")


def login_and_go_to_map(driver, email, password):
    driver.get("https://app.regrid.com/")
    time.sleep(3)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "user_email")))
        driver.find_element(By.ID, "user_email").send_keys(email)
        driver.find_element(By.ID, "user_password").send_keys(password)
        driver.find_element(By.NAME, "commit").click()
        WebDriverWait(driver, 10).until(EC.url_contains("/profile"))
        print("Login successful.")
    except Exception as e:
        print("Login form not present or already logged in.", e)
    try:
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "go-to-map"))).click()
        WebDriverWait(driver, 10).until(EC.url_contains("/us"))
        print("Map loaded.")
    except Exception as e:
        print("Already on the map page or 'Go to map' button not found.", e)


def extract_property_data(driver):
    """
    Extracts property field data from within the property tab.
    It iterates over every div.field element inside each panel-body and returns a list
    of dictionaries with keys: "key_text" and "value_text".
    """
    WebDriverWait(driver, 15).until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "div#property.tab-pane.active.in")))
    WebDriverWait(driver, 15).until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "div#property.tab-pane.active.in div.search-results")))
    property_tab = driver.find_element(
        By.CSS_SELECTOR, "div#property.tab-pane.active.in")
    try:
        search_results_div = property_tab.find_element(
            By.CLASS_NAME, "search-results")
    except Exception as ex:
        print("Could not find the search-results container; using entire property tab as fallback.", ex)
        search_results_div = property_tab

    html_dump = search_results_div.get_attribute("outerHTML")
    print("\n--- SEARCH RESULTS HTML DUMP (partial) ---\n",
          html_dump[:2000], "\n... [truncated] ...\n")

    panel_bodies = property_tab.find_elements(By.CSS_SELECTOR, ".panel-body")
    print(f"Found {len(panel_bodies)} panel-body element(s).")

    data_list = []
    for panel in panel_bodies:
        fields = panel.find_elements(By.CSS_SELECTOR, "div.field")
        for field in fields:
            entry = {"key_text": "", "value_text": ""}
            key_elems = field.find_elements(
                By.CSS_SELECTOR, "div.field-label div.key.subtle.small")
            if key_elems:
                entry["key_text"] = key_elems[0].text.strip()
            value_elems = field.find_elements(
                By.CSS_SELECTOR, "div.field-value div.value div.flex-row-between")
            if value_elems:
                entry["value_text"] = value_elems[0].text.strip()
            data_list.append(entry)
    return data_list


def main():
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()

    try:
        email = "rohabada@gmail.com"
        password = "Rohirrim#1312"
        login_and_go_to_map(driver, email, password)
        time.sleep(3)

        # Open CSV file in append mode to save progress safely.
        with open(output_filename, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=final_fieldnames)
            if write_header:
                writer.writeheader()
                csvfile.flush()

            for idx, row in final_buildings.iterrows():
                query = f"{row['address']}, {row['zip']}"
                print(
                    f"\nProcessing costarID {row['costarID']} with query: {query}")

                search_box = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "glmap-search-query")))
                search_box.clear()
                search_box.send_keys(query)
                time.sleep(1)

                # Wait for typeahead suggestions and try to click the first suggestion.
                suggestion_clicked = False
                try:
                    WebDriverWait(driver, 10).until(EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, "div.tt-menu.tt-open")))
                    suggestions = driver.find_elements(
                        By.CSS_SELECTOR, "div.tt-menu.tt-open div.tt-suggestion.tt-selectable")
                    if suggestions:
                        suggestions[0].click()
                        print("Clicked on the first suggestion.")
                        suggestion_clicked = True
                    else:
                        print("No suggestions found for this query.")
                except Exception as e:
                    print("Suggestion click failed; error:", e)

                # If no suggestion was clicked, record NA for all expected fields,
                # then clear the search box (using ESCAPE) and move on.
                if not suggestion_clicked:
                    print(
                        f"Filling NA for costarID {row['costarID']} because no suggestion was found.")
                    result = {
                        "costarID": row["costarID"],
                        "address": row["address"],
                        "zip": row["zip"],
                        "query": query
                    }
                    for field in expected_fields:
                        result[field] = "NA"
                    writer.writerow(result)
                    csvfile.flush()
                    try:
                        search_box.send_keys(Keys.ESCAPE)
                    except Exception as e:
                        print("Error sending ESCAPE to search box:", e)
                    search_box.clear()
                    time.sleep(2)
                    continue

                # If a suggestion was clicked, wait longer for property details to load.
                # Increased wait time to ensure the details are fully loaded before extraction.
                time.sleep(10)
                try:
                    panel_data = extract_property_data(driver)
                    print(
                        f"Extracted property data for costarID {row['costarID']}:")
                    print(panel_data)
                except Exception as ex:
                    print(
                        f"Failed to extract property data for costarID {row['costarID']}. Error: {ex}")
                    panel_data = []

                # Build result row, initializing expected fields as "NA".
                result = {
                    "costarID": row["costarID"],
                    "address": row["address"],
                    "zip": row["zip"],
                    "query": query
                }
                for field in expected_fields:
                    result[field] = "NA"

                # Update result row with any extracted panel data that match expected fields.
                for entry in panel_data:
                    key = entry.get("key_text", "").strip()
                    value = entry.get("value_text", "").strip()
                    if key in expected_fields and value:
                        result[key] = value

                writer.writerow(result)
                csvfile.flush()
                # Dismiss any lingering suggestions and clear the search box.
                try:
                    search_box.send_keys(Keys.ESCAPE)
                except Exception as e:
                    print("Error sending ESCAPE to search box:", e)
                search_box.clear()
                time.sleep(3)

        print(f"\n✅ Final property data saved to {output_filename}")

    finally:
        time.sleep(3)
        driver.quit()


if __name__ == "__main__":
    main()
