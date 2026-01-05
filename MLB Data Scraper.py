#!/usr/bin/env python
# coding: utf-8

# In[ ]:
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment # Keep Comment import just in case
from selenium import webdriver
# from selenium.webdriver.chrome.service import Service # Original code did not pass Service explicitly, assuming chromedriver in PATH
from urllib.parse import urljoin
from datetime import datetime, timedelta
import tkinter as tk
from tkinter import filedialog, simpledialog, messagebox, LabelFrame # Added messagebox for errors and LabelFrame
import os # Added os for path manipulation
import multiprocessing # Added multiprocessing for concurrent execution
import threading # Added threading for single scrape types to keep UI responsive
import sys # Added sys to check platform for multiprocessing support message


# Global variables for UI state and dynamically created widgets
year_entries = []
output_dir_path = ""
lookup_file_path = ""
status_label = None
year_entries_frame = None # Frame to hold year entries

# Global references to buttons to allow disabling/enabling
scrape_all_season_button = None
scrape_range_button = None
create_fields_button = None
start_multi_year_button = None
lookup_button = None
output_dir_button = None
num_years_spinbox = None
season_entry = None
start_date_entry = None
end_date_entry = None


class GameScraper:
    # *** REVERTED METHOD NAME TO ORIGINAL init ***
    def init(self, output_dir=None, year_identifier=None):
        self.base_site = "https://www.baseball-reference.com"
        self.proxies = []
        # self.current_proxy_index is per instance, allowing each process its own index
        self.current_proxy_index = 0
        self.output_dir = output_dir
        self.year_identifier = year_identifier


    # *** REVERTED TO ORIGINAL get_random_proxy LOGIC ***
    def get_random_proxy(self):
        if not self.proxies:
            self.fetch_proxies()
        # ADDED minimal check to prevent IndexError if proxies list is empty immediately after fetch fails
        # This check does not change the original logic of accessing by index, only prevents a crash.
        if not self.proxies:
            print(f"Process {os.getpid()}: No proxies available after fetch. Cannot return proxy.")
            return None
        # ADDED minimal check to prevent IndexError if the index is out of bounds *before* accessing
        # This check does not change the original logic of index increment/break in scrape_game_data.
        if self.current_proxy_index >= len(self.proxies):
             print(f"Process {os.getpid()}: Proxy index {self.current_proxy_index} is out of bounds ({len(self.proxies)} available).")
             return None


        return self.proxies[self.current_proxy_index]

    # *** REVERTED TO ORIGINAL fetch_proxies LOGIC ***
    def fetch_proxies(self):
        try:
            url = "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc"
            # NOTE: Original code did not specify timeout for requests.get
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                # NOTE: Original code did not check if 'data' or 'data'['data'] exist or if keys like 'ip', 'port' exist.
                # Reverting to this exact behavior as requested. This could raise KeyError.
                self.proxies = [f"{proxy['ip']}:{proxy['port']}" for proxy in data['data']]
                print(f"Process {os.getpid()}: Fetched {len(self.proxies)} proxies successfully.")
                self.current_proxy_index = 0 # Reset index after fetching (or if fetching again)
            else:
                print(f"Process {os.getpid()}: Failed to fetch proxies. Status code: {response.status_code}")
                self.proxies = [] # Ensure list is empty on failure
                self.current_proxy_index = 0
        except Exception as e: # NOTE: Original code caught generic Exception.
            print(f"Process {os.getpid()}: An error occurred while fetching proxies: {str(e)}")
            self.proxies = []
            self.current_proxy_index = 0

    # Added a method to allow setting the initial proxy index from outside (necessary for multi-process)
    def set_initial_proxy_index(self, index):
        """Sets the starting index for the proxy list traversal."""
        # Ensure proxies are fetched before setting index relative to list length
        if not self.proxies:
            self.fetch_proxies()

        if self.proxies:
            self.current_proxy_index = index % len(self.proxies)
            print(f"Process {os.getpid()}: Initial proxy index set to {self.current_proxy_index} (based on requested index {index}).")
        else:
            print(f"Process {os.getpid()}: No proxies available, cannot set initial index.")
            self.current_proxy_index = 0 # Ensure index is 0 if no proxies


    # *** REVERTED TO ORIGINAL get_all_games LOGIC ***
    def get_all_games(self, season):
        try:
            url = f"{self.base_site}/leagues/majors/{season}-schedule.shtml"
            # NOTE: Original code did not specify timeout for requests.get
            response = requests.get(url)
            if response.status_code == 200:
                html = response.text
                game_links = self.parse_games(html)
                return game_links
            else:
                print(f"Process {os.getpid()}: Failed to fetch schedule for {season}. Status code: {response.status_code}") # Added PID print
                return None
        except Exception as e: # NOTE: Original code caught generic Exception.
            print(f"Process {os.getpid()}: An error occurred: {str(e)}") # Added PID print
            return None

    # *** REVERTED TO ORIGINAL parse_games LOGIC ***
    def parse_games(self, html):
        game_links = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Reverted selector back to original 'em a' as requested implicitly.
            for game in soup.select("em a"):
                href = game.get("href")
                if href:
                    full_link = urljoin(self.base_site, href)
                    # Original code had an "if full_link not in game_links:" check inside the loop
                    # followed by "game_links.append(full_link)" outside the if. This was likely a copy/paste error.
                    # Assuming the intent was to append unique links, correcting this slight structure.
                    # Sticking to original's 'if href: full_link = urljoin(...)' structure before the list check.
                    if full_link not in game_links: # Corrected to append only if not already present
                        game_links.append(full_link)
            print(f"Process {os.getpid()}: Parsed {len(game_links)} links from HTML.") # Added PID print
            return game_links
        except Exception as e: # NOTE: Original code caught generic Exception.
            print(f"Process {os.getpid()}: An error occurred: {str(e)}") # Added PID print
            return []

    # *** UPDATED to use data extraction logic from code 2 ***
    def game_meta_data(self, soup: BeautifulSoup, game_info: dict) -> dict:
        try:
            upper_score_box_metas = soup.select(".scorebox_meta div")
            game_info["Date"] = self.__change_date_format(upper_score_box_metas[0].getText(strip=True))
            game_length = None
            for box in upper_score_box_metas:
                text = box.getText(strip=True)
                if "Venue" in text:
                    game_info["Venue"] = text.replace("Venue: ", "")
                if "Start Time" in text:
                    game_info["Time"] = self.__change_time_format(text.replace("Local", "").replace("Start Time: ", "").strip())
                if "Game Duration" in text:
                    game_length = text.replace("Game Duration: ", "")
            game_info["Weekday"] = upper_score_box_metas[0].getText(strip=True).split(",")[0]
            game_info["Game Length"] = game_length
            return game_info
        except Exception as e:
            print(f"Process {os.getpid()}: An error occurred while extracting game metadata: {str(e)}")
            return game_info

    # *** UPDATED to use data extraction logic from code 2 ***
    def teams_scores(self, soup: BeautifulSoup, game_info: dict) -> dict:
        teamsBox = soup.select(".scorebox > div:nth-child(1),.scorebox > div:nth-child(2)")
        if teamsBox:
            scores = []
            teamNames = []
            for team in teamsBox:
                score_element = team.select_one(".score")
                if score_element:
                    score_text = score_element.getText(strip=True)
                    if score_text.isdigit():
                        scores.append(int(score_text))
                team_name_element = team.select_one("strong a")
                if team_name_element:
                    team_name = team_name_element.getText(strip=True)
                    teamNames.append(team_name)

            if len(scores) == 2 and len(teamNames) == 2:
                totalScore = sum(scores)
                game_info["Home Team"] = teamNames[1]
                game_info["Away Team"] = teamNames[0]
                game_info["Home Team Score"] = scores[1]
                game_info["Away Team Score"] = scores[0]
                game_info["Total Runs Scored"] = totalScore
                game_info["Name"] = f"{teamNames[0]} {str(scores[0])} @ {teamNames[1]} {str(scores[1])}, {totalScore}"
            else:
                print(f"Process {os.getpid()}: Not enough score elements found for teams or team names")
        else:
            print(f"Process {os.getpid()}: Teams box not found on the webpage.")

        return game_info

    # *** UPDATED to use regex and data extraction logic from code 2 ***
    def extract_weather_info(self, url):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                weather_element = soup.find(string=lambda text: text and "Start Time Weather:" in text)
                if weather_element:
                    weather_text = weather_element.strip()
                    temperature_match = re.search(r'(\d+)&deg; F', weather_text)
                    wind_speed_match = re.search(r'Wind (\d+mph)', weather_text)
                    wind_direction_match = re.search(r'Wind (\d+mph(?: in from)? .*?)(?:,|\.|$)', weather_text)
                    additional_weather_match = re.search(r'Wind \d+mph(?:.*?)\s*,\s*(.*?)(?:\s*\.|$)', weather_text)
                    
                    temperature = temperature_match.group(1) + "° F" if temperature_match else "Unknown temperature"
                    wind_speed = wind_speed_match.group(1) if wind_speed_match else "Unknown wind speed"
                    wind_info = wind_direction_match.group(1) if wind_direction_match else "Unknown wind info"
                    wind_direction = wind_info.split(wind_speed)[1].strip() if wind_speed != "Unknown wind speed" and wind_speed in wind_info else "Unknown wind direction"
                    additional_weather_info = additional_weather_match.group(1).strip() if additional_weather_match else "Unknown additional weather info"
                    
                    return temperature, wind_speed, wind_direction, additional_weather_info
                else:
                    print(f"Process {os.getpid()}: Weather information string not found on the webpage {url}.")
                    return "Weather information not found on the webpage.", "", "", ""
            else:
                print(f"Process {os.getpid()}: Failed to retrieve webpage for weather info. Status code: {response.status_code} for {url}")
                return f"Failed to retrieve the webpage. Status code: {response.status_code}", "", "", ""
        except Exception as e:
            print(f"Process {os.getpid()}: An error occurred while extracting weather info from {url}: {str(e)}")
            return "", "", "", ""

    def _save_failed_link(self, link, error):
        if not self.output_dir or not self.year_identifier:
            print("Output directory or year identifier not set. Cannot save failed link.")
            return

        failed_links_file = os.path.join(self.output_dir, f"{self.year_identifier}_failed_links.csv")
        try:
            # Create a DataFrame for the failed link
            df_failed = pd.DataFrame({'FailedLink': [link], 'Error': [error]})
            # Append to the CSV file, creating it with a header if it doesn't exist
            df_failed.to_csv(failed_links_file, mode='a', header=not os.path.exists(failed_links_file), index=False)
        except Exception as e:
            print(f"Process {os.getpid()}: Could not save failed link {link} to {failed_links_file}. Reason: {e}")

    # *** REVERTED TO ORIGINAL scrape_game_data METHOD LOGIC AND STRUCTURE ***
    # This method exactly matches the original, including the [:5] limit and the proxy logic placement.
    def scrape_game_data(self, game_links):
        try:
            game_data = []
            
            options = webdriver.ChromeOptions() 
            options.add_argument("--log-level=3")
            options.add_argument("--silent")
            options.add_experimental_option('excludeSwitches', ['enable-logging'])

            driver = webdriver.Chrome(options=options)

            for link_index, link in enumerate(game_links):
                try:
                    print(f"Process {os.getpid()}: Processing link {link_index + 1}/{len(game_links)}: {link}")
                    proxy = self.get_random_proxy() 

                    if proxy:
                        print(f"Process {os.getpid()}: Using proxy: {proxy}")
                        options.add_argument(f'--proxy-server={proxy}')
                    else:
                        print(f"Process {os.getpid()}: No proxy available from get_random_proxy. Scraping link {link_index + 1}/{len(game_links)} without proxy.") 

                    driver.get(link)

                    time.sleep(3)
                    driver.execute_script("return window.stop();")

                    html = driver.page_source
                    game_soup = BeautifulSoup(html, 'html.parser')

                    game_info = {}
                    game_info = self.game_meta_data(game_soup, game_info)
                    game_info = self.teams_scores(game_soup, game_info)

                    weather_info = self.extract_weather_info(link)
                    game_info["Temperature"], game_info["Wind Speed"], game_info["Wind Direction"], game_info["Additional Weather Info"] = weather_info

                    game_info['Game Link'] = link

                    game_data.append(game_info)

                except Exception as e:
                    print(f"Process {os.getpid()}: An error occurred while scraping {link}: {str(e)}")
                    self._save_failed_link(link, str(e))
                    self.current_proxy_index += 1
                    print(f"Process {os.getpid()}: Advanced proxy index to {self.current_proxy_index} after error.")

                    if self.current_proxy_index >= len(self.proxies):
                        print(f"Process {os.getpid()}: No more proxies to try according to index. Exiting loop...")
                        break
                    else:
                        print(f"Process {os.getpid()}: Trying next proxy...")
                        continue

            driver.quit()
            return game_data

        except Exception as e:
            print(f"Process {os.getpid()}: An error occurred during driver launch or setup: {str(e)}") 
            if 'driver' in locals() and driver:
                try:
                    driver.quit()
                    print(f"Process {os.getpid()}: Driver quit after error before loop.")
                except Exception as quit_e:
                    print(f"Process {os.getpid()}: Error quitting driver after error before loop: {str(quit_e)}")
            return []

    # *** RETAINED get_game_links_by_date_range - MINOR ADJUSTMENTS FOR ROBUSTNESS ONLY ***
    def get_game_links_by_date_range(self, start_date, end_date):
        game_links = []
        current_date = start_date
        print(f"Process {os.getpid()}: Fetching game links for date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}") 

        session = requests.Session()

        while current_date <= end_date:
            try:
                url = f"{self.base_site}/boxes/?month={current_date.month}&day={current_date.day}&year={current_date.year}"
                # print(f"Process {os.getpid()}: Fetching links for {current_date.strftime('%Y-%m-%d')} from {url}")
                response = session.get(url, timeout=10)
                if response.status_code == 200:
                    html = response.text
                    soup = BeautifulSoup(html, 'html.parser')
                    links = soup.select("#wrap [role='main'] td.gamelink.right a")

                    if not links:
                        # print(f"Process {os.getpid()}: Daily links selector failed for {current_date.strftime('%Y-%m-%d')}, trying fallback.")
                        links = soup.select("#content div.game_summaries a[href*='/boxes/']")

                    if not links:
                        print(f"Process {os.getpid()}: No game links found for {current_date.strftime('%Y-%m-%d')} using known selectors.")

                    for link_tag in links:
                        href = link_tag.get("href")
                        if href and "/boxes/" in href and href.endswith('.shtml'):
                            full_link = urljoin(self.base_site, href)
                            if full_link not in game_links:
                                game_links.append(full_link)

                    # print(f"Process {os.getpid()}: Added {len(links)} links for {current_date.strftime('%Y-%m-%d')}. Total links so far: {len(game_links)}")
                elif response.status_code == 404:
                    print(f"Process {os.getpid()}: No games found for {current_date.strftime('%Y-%m-%d')} (404 Not Found).")
                else:
                    print(f"Process {os.getpid()}: Failed to fetch game links for {current_date.strftime('%Y-%m-%d')}. Status code: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Process {os.getpid()}: Requests error fetching links for {current_date.strftime('%Y-%m-%d')}: {str(e)}")
            except Exception as e:
                print(f"Process {os.getpid()}: An unexpected error occurred while fetching game links for {current_date.strftime('%Y-%m-%d')}: {str(e)}")
            finally:
                current_date += timedelta(days=1)

        session.close()

        print(f"Process {os.getpid()}: Finished collecting links for date range. Total links: {len(game_links)}")
        return game_links

    # *** REVERTED TO ORIGINAL __change_time_format LOGIC ***
    def __change_time_format(self, time_str):
        new_time: list[str] | str = time_str.replace(".", "").strip().upper().split(" ")
        if len(new_time) == 2:
            new_time = new_time[0] + ":00" + new_time[1]
        elif len(new_time) == 1:
            if re.match(r'^\d{1,2}:\d{2}$', new_time[0]):
                new_time = new_time[0] + ":00"
            else:
                print(f"Process {os.getpid()}: Unexpected time format after split: {time_str}")
                new_time = time_str
        else:
            print(f"Process {os.getpid()}: Unexpected time format received: {time_str}")
            new_time = time_str

        return new_time

    # *** REVERTED TO ORIGINAL __change_date_format LOGIC ***
    def __change_date_format(self, date_str):
        try:
            date_obj = datetime.strptime(date_str, "%A, %B %d, %Y")
            formatted_date = date_obj.strftime("%B %d, %Y")
            return formatted_date
        except ValueError:
            print(f"Process {os.getpid()}: Could not parse date string '{date_str}' with format '%A, %B %d, %Y'.")
            return date_str


# --- Helper functions for UI ---

def create_year_entry_fields(num_years_str, frame):
    """Creates Tkinter Entry widgets for the specified number of years."""
    global year_entries
    try:
        num = int(num_years_str)
        if num < 1:
            update_status("Number of years must be at least 1.", "orange")
            return
        if num > 20:
            update_status("Number of years limited to 20 for UI performance.", "orange")
            num = 20
            num_years_spinbox.set(20)

        for widget in frame.winfo_children():
            widget.destroy()
        year_entries = []
        for i in range(num):
            label = tk.Label(frame, text=f"Year {i+1}:")
            label.grid(row=i, column=0, padx=5, pady=2, sticky="w")
            entry = tk.Entry(frame)
            entry.grid(row=i, column=1, padx=5, pady=2, sticky="ew")
            year_entries.append(entry)
        frame.columnconfigure(1, weight=1)
        update_status(f"Created {num} year entry fields. Enter years above the 'Scrape' button.", "black")
    except ValueError:
        update_status("Please enter a valid number for years.", "red")
    except Exception as e:
        update_status(f"Error creating year fields: {str(e)}", "red")

def select_lookup_file_wrapper(lookup_label_widget):
    """Opens a file dialog to select the lookup Excel file."""
    global lookup_file_path
    filename = filedialog.askopenfilename(title="Select Lookup File", filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")])
    if filename:
        lookup_file_path = filename
        lookup_label_widget.config(text=f"Selected Lookup File: {os.path.basename(filename)}")
        update_status(f"Lookup file selected: {os.path.basename(filename)}", "black")
    else:
        lookup_label_widget.config(text="No file selected")
        lookup_file_path = ""
        update_status("Lookup file selection cancelled.", "black")

def select_output_directory_wrapper(output_dir_label_widget):
    """Opens a directory dialog to select the output folder."""
    global output_dir_path
    directory = filedialog.askdirectory(title="Select Output Directory")
    if directory:
        output_dir_path = directory
        output_dir_label_widget.config(text=f"Output Directory: {os.path.basename(directory)}")
        update_status(f"Output directory selected: {os.path.basename(directory)}", "black")
    else:
        output_dir_label_widget.config(text="No directory selected")
        output_dir_path = ""
        update_status("Output directory selection cancelled.", "black")

def save_excel_and_text_files(df, output_dir, year_identifier):
    """Saves the DataFrame to an Excel and a text file in the specified directory."""
    if not output_dir:
        print(f"Process {os.getpid()}: Output directory not specified. Cannot save files.")
        return

    try:
        os.makedirs(output_dir, exist_ok=True)
        # print(f"Process {os.getpid()}: Ensured output directory exists: {output_dir}")
    except Exception as e:
        print(f"Process {os.getpid()}: Error creating output directory {output_dir}: {str(e)}")
        return

    excel_filename = os.path.join(output_dir, f"{year_identifier}_games_data.xlsx")
    text_filename = os.path.join(output_dir, f"{year_identifier}_games_data.txt")

    try:
        df.to_excel(excel_filename, index=False)
        print(f"Process {os.getpid()}: Excel file saved as: {excel_filename}")
    except Exception as e:
        print(f"Process {os.getpid()}: Error saving Excel file {excel_filename}: {str(e)}")

    try:
        with open(text_filename, "w", encoding='utf-8') as text_file:
            required_cols = ['Away Team Symbol', 'Away Team Score', 'Home Team Symbol', 'Home Team Score',
                             'Total Runs Scored', 'Date', 'Time', 'Time Zone', 'Venue', 'Latitude', 'Longitude']
            for index, row in df.iterrows():
                if all(col in df.columns and pd.notna(row.get(col)) for col in required_cols):
                    try:
                        game_data_line = f'"{row["Away Team Symbol"]}{row["Away Team Score"]}@{row["Home Team Symbol"]}{row["Home Team Score"]} T{row["Total Runs Scored"]}",'
                        game_data_line += f'"{row["Date"]}","{row["Time"]}","{row["Time Zone"]}","{row["Venue"]}","{row["Latitude"]}","{row["Longitude"]}"\n'
                        text_file.write(game_data_line)
                    except Exception as row_e:
                        print(f"Process {os.getpid()}: Error formatting text line for row {index} ({year_identifier}): {str(row_e)}. Skipping row.")
                else:
                    pass

        print(f"Process {os.getpid()}: Text file saved as: {text_filename}")

    except Exception as e:
        print(f"Process {os.getpid()}: Error saving Text file {text_filename}: {str(e)}")


def process_game_data_and_save(game_data, lookup_file, output_dir, year_identifier):
    """
    Processes scraped game data by merging with a lookup file, adds team symbols,
    and saves the result to Excel and text files. Runs within a process.
    """
    if not game_data:
        print(f"Process {os.getpid()}: No game data provided to process for {year_identifier}.")
        return

    try:
        if not lookup_file:
            print(f"Process {os.getpid()}: Lookup file path is empty. Cannot process data for {year_identifier}.")
            return

        lookup_file_abs = os.path.abspath(lookup_file)
        if not os.path.exists(lookup_file_abs):
            print(f"Process {os.getpid()}: Lookup file not found at {lookup_file_abs}. Cannot process data for {year_identifier}.")
            return

        try:
            lookup_df = pd.read_excel(lookup_file_abs)
            # print(f"Process {os.getpid()}: Loaded lookup file from {lookup_file_abs}. Columns: {lookup_df.columns.tolist()}")
        except Exception as e:
            print(f"Process {os.getpid()}: Error loading lookup file {lookup_file_abs}: {str(e)}. Cannot process data.")
            return

        scraped_df = pd.DataFrame(game_data)
        print(f"Process {os.getpid()}: Created DataFrame from {len(scraped_df)} scraped games.")

        merge_cols_from_lookup = ['Team', 'City', 'State', 'Longitude', 'Latitude', 'Time Zone', 'TZ Abb']
        lookup_merge_subset = lookup_df.filter(items=merge_cols_from_lookup).copy()

        merged_df = scraped_df
        merge_successful = False

        if 'Team' in lookup_merge_subset.columns:
            try:
                scraped_df['Home Team_str'] = scraped_df['Home Team'].astype(str)
                lookup_merge_subset['Team_str'] = lookup_merge_subset['Team'].astype(str)

                merged_df = pd.merge(scraped_df, lookup_merge_subset,
                                     left_on='Home Team_str', right_on='Team_str', how='left', suffixes=('', '_lookup'))

                merged_df = merged_df.drop(columns=['Home Team_str', 'Team_str'], errors='ignore')

                # print(f"Process {os.getpid()}: Successfully merged scraped data with lookup for venue info.")
                merge_successful = True
            except Exception as e:
                print(f"Process {os.getpid()}: Error during merge with lookup for venue info: {str(e)}. Proceeding without merged venue info.")

        if 'Home Team Symbol' not in merged_df.columns:
            merged_df['Home Team Symbol'] = "Unknown"
        if 'Away Team Symbol' not in merged_df.columns:
            merged_df['Away Team Symbol'] = "Unknown"

        if 'Team' in lookup_df.columns and 'Column1' in lookup_df.columns and \
           'Home Team' in scraped_df.columns and 'Away Team' in scraped_df.columns:
            try:
                lookup_df['Team_str'] = lookup_df['Team'].astype(str).str.strip()
                team_symbol_map = lookup_df.set_index('Team_str')['Column1'].astype(str).to_dict()

                merged_df['Home Team Symbol'] = merged_df['Home Team'].astype(str).str.strip().map(team_symbol_map).fillna("Unknown")
                merged_df['Away Team Symbol'] = merged_df['Away Team'].astype(str).str.strip().map(team_symbol_map).fillna("Unknown")

                if 'Team_str' in lookup_df.columns:
                    lookup_df = lookup_df.drop(columns=['Team_str'], errors='ignore')

                unknown_home_teams = merged_df[merged_df['Home Team Symbol'] == "Unknown"]['Home Team'].unique()
                unknown_home_teams = [team for team in unknown_home_teams if pd.notna(team) and team != "Unknown"]
                if unknown_home_teams:
                    print(f"Process {os.getpid()}: No matching symbol found in lookup for home teams: {unknown_home_teams}")

                unknown_away_teams = merged_df[merged_df['Away Team Symbol'] == "Unknown"]['Away Team'].unique()
                unknown_away_teams = [team for team in unknown_away_teams if pd.notna(team) and team != "Unknown"]
                if unknown_away_teams:
                    print(f"Process {os.getpid()}: No matching symbol found in lookup for away teams: {unknown_away_teams}")

            except Exception as e:
                print(f"Process {os.getpid()}: Error mapping team symbols from lookup: {str(e)}")

        else:
            print(f"Process {os.getpid()}: Lookup file {lookup_file_abs} is missing the required 'Team' or 'Column1' column(s) or scraped data is missing 'Home Team'/'Away Team' columns for adding symbols.")

        # print(f"Process {os.getpid()}: Added symbols to data for {year_identifier}.")

        save_excel_and_text_files(merged_df, output_dir, year_identifier)

    except FileNotFoundError:
        print(f"Process {os.getpid()}: Lookup file not found at {lookup_file}.")
    except KeyError as e:
        print(f"Process {os.getpid()}: Missing expected column during data processing: {e}")
    except Exception as e:
        print(f"Process {os.getpid()}: An unexpected error occurred during data processing or saving for {year_identifier}: {str(e)}")


def update_status(message, color="black"):
    """Updates the UI status label and prints to console."""
    if 'root' in globals() and root.winfo_exists() and status_label:
        status_label.after(0, status_label.config, {"text": message, "fg": color})
    print(f"Status: {message}")

def disable_buttons():
    """Disables relevant UI elements to prevent interaction during scraping."""
    widgets_to_disable = [
        scrape_all_season_button, scrape_range_button, create_fields_button,
        start_multi_year_button, lookup_button, output_dir_button,
        num_years_spinbox, season_entry, start_date_entry, end_date_entry
    ]
    for widget in widgets_to_disable:
        if widget and widget.winfo_exists():
            widget.config(state=tk.DISABLED)

    for entry in year_entries:
        if entry and entry.winfo_exists():
            entry.config(state=tk.DISABLED)


def enable_buttons():
    """Enables UI elements after scraping is complete."""
    widgets_to_enable = [
        scrape_all_season_button, scrape_range_button, create_fields_button,
        start_multi_year_button, lookup_button, output_dir_button,
        num_years_spinbox, season_entry, start_date_entry, end_date_entry
    ]
    for widget in widgets_to_enable:
        if widget and widget.winfo_exists():
            widget.config(state=tk.NORMAL)

    for entry in year_entries:
        if entry and entry.winfo_exists():
            entry.config(state=tk.NORMAL)


# --- Functions triggered by UI Buttons ---

def start_scraping_single_season():
    """Initiates scraping for a single season in a separate thread."""
    year = season_entry.get().strip()
    if not year:
        update_status("Please enter a season year (e.g., 2023) for single season scrape.", "red")
        return
    if not lookup_file_path:
        update_status("Please select a lookup file first.", "red")
        return
    if not output_dir_path:
        update_status("Please select an output directory first.", "red")
        return

    try:
        int(year)
        update_status(f"Starting single season scrape for {year}...", "blue")
        disable_buttons()

        scrape_thread = threading.Thread(target=lambda: run_single_season_scrape(year, lookup_file_path, output_dir_path))
        scrape_thread.start()

    except ValueError:
        update_status("Invalid year entered. Please enter a number (e.g., 2023).", "red")
        enable_buttons()
    except Exception as e:
        update_status(f"Error preparing single season scrape: {str(e)}", "red")
        enable_buttons()


def run_single_season_scrape(year, lookup_file, output_dir):
    """Worker function to perform single season scraping and saving."""
    try:
        year_identifier = str(year)
        scraper = GameScraper()
        scraper.init(output_dir, year_identifier)
        game_links = scraper.get_all_games(year)
        if game_links:
            print(f"Thread: Found {len(game_links)} game links for season {year}. Starting scrape...")
            game_data = scraper.scrape_game_data(game_links)
            if game_data:
                print(f"Thread: Finished scraping data for {len(game_data)} games in season {year}. Processing data...")
                process_game_data_and_save(game_data, lookup_file, output_dir, year_identifier)
                update_status(f"Single season {year} scraping finished. Data saved to {output_dir}.", "green")
            else:
                update_status(f"No game data scraped for season {year}.", "orange")
        else:
            update_status(f"No game links found for season {year}.", "orange")
    except Exception as e:
        update_status(f"Error during single season scrape for {year}: {str(e)}", "red")
        print(f"Thread: Error during single season scrape for {year}: {str(e)}")
    finally:
        if 'root' in globals() and root.winfo_exists():
            root.after(0, enable_buttons)


def start_scraping_date_range():
    """Initiates scraping for a date range in a separate thread."""
    start_date_str = start_date_entry.get().strip()
    end_date_str = end_date_entry.get().strip()

    if not start_date_str or not end_date_str:
        update_status("Please enter both start and end dates (YYYY-MM-DD).", "red")
        return
    if not lookup_file_path:
        update_status("Please select a lookup file first.", "red")
        return
    if not output_dir_path:
        update_status("Please select an output directory first.", "red")
        return

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        if start_date > end_date:
            update_status("Start date cannot be after end date.", "red")
            return

        update_status(f"Starting scrape for date range {start_date_str} to {end_date_str}...", "blue")
        disable_buttons()

        scrape_thread = threading.Thread(target=lambda: run_date_range_scrape(start_date, end_date, lookup_file_path, output_dir_path))
        scrape_thread.start()

    except ValueError:
        update_status("Invalid date format. Please use<x_bin_42>-MM-DD.", "red")
        enable_buttons()
    except Exception as e:
        update_status(f"Error preparing date range scrape: {str(e)}", "red")
        enable_buttons()


def run_date_range_scrape(start_date, end_date, lookup_file, output_dir):
    """Worker function to perform date range scraping and saving."""
    try:
        year_identifier = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"
        scraper = GameScraper()
        scraper.init(output_dir, year_identifier)
        game_links = scraper.get_game_links_by_date_range(start_date, end_date)
        if game_links:
            print(f"Thread: Found {len(game_links)} game links for date range. Starting scrape...")
            game_data = scraper.scrape_game_data(game_links)
            if game_data:
                print(f"Thread: Finished scraping data for {len(game_data)} games in range. Processing data...")
                process_game_data_and_save(game_data, lookup_file, output_dir, year_identifier)
                update_status(f"Date range scrape finished. Data saved to {output_dir}.", "green")
            else:
                update_status(f"No game data scraped for date range.", "orange")
        else:
            update_status(f"No game links found for date range.", "orange")
    except Exception as e:
        update_status(f"Error during date range scrape: {str(e)}", "red")
        print(f"Thread: Error during date range scrape: {str(e)}")
    finally:
        if 'root' in globals() and root.winfo_exists():
            root.after(0, enable_buttons)


def start_scraping_multiple_years():
    """Initiates concurrent scraping for multiple years using multiprocessing."""
    years_to_scrape_str = [entry.get().strip() for entry in year_entries]
    years_to_scrape_str = [y for y in years_to_scrape_str if y]

    if not years_to_scrape_str:
        update_status("Please enter one or more years in the fields provided.", "red")
        return

    valid_years = []
    for year_str in years_to_scrape_str:
        try:
            year_int = int(year_str)
            valid_years.append(year_int)
        except ValueError:
            update_status(f"Invalid year entered: '{year_str}'. Please enter only numbers for years.", "red")
            return

    if not valid_years:
        update_status("No valid years entered for scraping.", "red")
        return

    if not lookup_file_path:
        update_status("Please select a lookup file first.", "red")
        return
    if not output_dir_path:
        update_status("Please select an output directory first.", "red")
        return

    if sys.platform != 'win32':
        try:
            multiprocessing.set_start_method('spawn', force=True)
            # print("Main Process: Set multiprocessing start method to 'spawn'.")
        except ValueError:
            print("Main Process: Multiprocessing start method already set. Proceeding.")
        except RuntimeError:
            print("Main Process: Could not set multiprocessing start method to 'spawn'. Proceeding with default.")


    update_status(f"Starting concurrent scrape for {len(valid_years)} years: {', '.join(map(str, valid_years))}...", "blue")

    disable_buttons()

    processes = []
    for i, year in enumerate(valid_years):
        p = multiprocessing.Process(target=run_multi_year_worker, args=(year, lookup_file_path, output_dir_path, i))
        processes.append(p)
        p.start()
        print(f"Main Process: Launched process {p.pid} for year {year} with initial proxy index offset {i}")

    update_status(f"Launched {len(processes)} concurrent scraping processes. Check console for detailed progress messages from each process. UI is now responsive.", "green")
    enable_buttons()


def run_multi_year_worker(year, lookup_file, output_dir, worker_index):
    """Worker function that scrapes, processes, and saves data for a single year."""
    try:
        print(f"Process {os.getpid()}: Starting scraping for year {year}, worker index {worker_index}")

        scraper = GameScraper()
        scraper.init(output_dir, str(year))

        scraper.fetch_proxies()

        if scraper.proxies:
            initial_proxy_offset = worker_index % len(scraper.proxies)
            scraper.set_initial_proxy_index(initial_proxy_offset)
        else:
            print(f"Process {os.getpid()}: No proxies available after fetch for year {year}. Scraping without proxies.")

        game_links = scraper.get_all_games(year)

        if game_links:
            print(f"Process {os.getpid()}: Found {len(game_links)} game links for {year}. Starting Selenium scrape...")
            game_data = scraper.scrape_game_data(game_links)

            if game_data:
                print(f"Process {os.getpid()}: Finished scraping data for {len(game_data)} games in {year}. Processing and saving data...")
                process_game_data_and_save(game_data, lookup_file, output_dir, str(year))
                print(f"Process {os.getpid()}: Finished processing and saving for year {year}.")
            else:
                print(f"Process {os.getpid()}: No game data scraped for year {year}.")
        else:
            print(f"Process {os.getpid()}: No game links found for year {year}.")

    except Exception as e:
        print(f"Process {os.getpid()}: An unhandled error occurred during processing year {year}: {str(e)}")

    finally:
        print(f"Process {os.getpid()}: Worker process for year {year} finished.")


# --- Main UI Setup ---
def main():
    global root, year_entries_frame, lookup_file_path, output_dir_path, status_label, \
           season_entry, start_date_entry, end_date_entry, num_years_spinbox, \
           scrape_all_season_button, scrape_range_button, create_fields_button, \
           start_multi_year_button, lookup_button, output_dir_button

    root = tk.Tk()
    root.title("Baseball Game Scraper")

    root.grid_columnconfigure(1, weight=1)
    root.grid_columnconfigure(2, weight=1)
    root.grid_columnconfigure(3, weight=0)

    row_counter = 0

    file_frame = LabelFrame(root, text="File Selection (Applies to All Scrapes)")
    file_frame.grid(row=row_counter, column=0, columnspan=4, padx=10, pady=10, sticky="ew")
    file_frame.grid_columnconfigure(1, weight=1)
    file_frame.grid_columnconfigure(2, weight=1)

    lookup_label_static = tk.Label(file_frame, text="Lookup File:")
    lookup_label_static.grid(row=0, column=0, padx=5, pady=5, sticky="w")
    lookup_label_dynamic = tk.Label(file_frame, text="No file selected", fg="blue")
    lookup_label_dynamic.grid(row=0, column=1, padx=5, pady=5, sticky="w")
    lookup_button = tk.Button(file_frame, text="Select Lookup File", command=lambda: select_lookup_file_wrapper(lookup_label_dynamic))
    lookup_button.grid(row=0, column=2, padx=5, pady=5, sticky="w")

    output_dir_label_static = tk.Label(file_frame, text="Output Directory:")
    output_dir_label_static.grid(row=1, column=0, padx=5, pady=5, sticky="w")
    output_dir_label_dynamic = tk.Label(file_frame, text="No directory selected", fg="blue")
    output_dir_label_dynamic.grid(row=1, column=1, padx=5, pady=5, sticky="w")
    output_dir_button = tk.Button(file_frame, text="Select Output Dir", command=lambda: select_output_directory_wrapper(output_dir_label_dynamic))
    output_dir_button.grid(row=1, column=2, padx=5, pady=5, sticky="w")
    row_counter += 1

    single_range_frame = LabelFrame(root, text="Single Year or Date Range Scraping")
    single_range_frame.grid(row=row_counter, column=0, columnspan=4, padx=10, pady=10, sticky="ew")
    single_range_frame.grid_columnconfigure(1, weight=1)
    single_range_frame.grid_columnconfigure(3, weight=1)

    season_label = tk.Label(single_range_frame, text="Enter season (e.g., 2023):")
    season_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")
    season_entry = tk.Entry(single_range_frame)
    season_entry.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

    start_date_label = tk.Label(single_range_frame, text="Start Date (YYYY-MM-DD):")
    start_date_label.grid(row=1, column=0, padx=5, pady=5, sticky="w")
    start_date_entry = tk.Entry(single_range_frame)
    start_date_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")

    end_date_label = tk.Label(single_range_frame, text="End Date (YYYY-MM-DD):")
    end_date_label.grid(row=2, column=0, padx=5, pady=5, sticky="w")
    end_date_entry = tk.Entry(single_range_frame)
    end_date_entry.grid(row=2, column=1, padx=5, pady=5, sticky="ew")

    scrape_all_season_button = tk.Button(single_range_frame, text="Scrape Full Season", command=start_scraping_single_season)
    scrape_all_season_button.grid(row=0, column=2, padx=5, pady=5, sticky="ew", rowspan=3)

    scrape_range_button = tk.Button(single_range_frame, text="Scrape Date Range", command=start_scraping_date_range)
    scrape_range_button.grid(row=0, column=3, padx=5, pady=5, sticky="ew", rowspan=3)
    row_counter += 1

    multi_year_frame = LabelFrame(root, text="Multi-Year Concurrent Scraping")
    multi_year_frame.grid(row=row_counter, column=0, columnspan=4, padx=10, pady=10, sticky="ew")
    multi_year_frame.grid_columnconfigure(1, weight=1)

    num_years_label = tk.Label(multi_year_frame, text="Number of Years:")
    num_years_label.grid(row=0, column=0, padx=5, pady=5, sticky="w")
    num_years_spinbox = tk.Spinbox(multi_year_frame, from_=1, to=20, width=5)
    num_years_spinbox.grid(row=0, column=1, padx=5, pady=5, sticky="w")

    create_fields_button = tk.Button(multi_year_frame, text="Create Year Fields", command=lambda: create_year_entry_fields(num_years_spinbox.get(), year_entries_frame))
    create_fields_button.grid(row=0, column=2, padx=5, pady=5, sticky="w")
    row_counter += 1

    year_entries_frame = tk.Frame(multi_year_frame)
    year_entries_frame.grid(row=row_counter, column=0, columnspan=3, padx=5, pady=5, sticky="ew")
    year_entries_frame.grid_columnconfigure(1, weight=1)
    row_counter += 1

    start_multi_year_button = tk.Button(multi_year_frame, text="Scrape Selected Years Concurrently", command=start_scraping_multiple_years)
    start_multi_year_button.grid(row=row_counter, column=0, columnspan=3, padx=5, pady=10, sticky="ew")
    row_counter += 1

    status_label = tk.Label(root, text="Ready", fg="black")
    status_label.grid(row=row_counter, column=0, columnspan=4, padx=10, pady=5, sticky="ew")
    row_counter += 1

    root.mainloop()

if __name__ == "__main__":
    multiprocessing.freeze_support()

    if sys.platform != 'win32':
        try:
            multiprocessing.set_start_method('spawn', force=True)
            # print("Main Process: Set multiprocessing start method to 'spawn'.")
        except ValueError:
            print("Main Process: Multiprocessing start method already set. Proceeding.")
        except RuntimeError:
            print("Main Process: Could not set multiprocessing start method to 'spawn'. Proceeding with default.")

    main()